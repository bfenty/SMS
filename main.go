package main

import (
	"database/sql"
	"fmt"
	"strings"

	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sfreiberg/gotwilio"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

// Initialize the DB Variable
var db *sql.DB

// initialize other variables
var warning int
var overdue int

// Define a struct for the map key
type SorterPhoneKey struct {
	Sorter string
	Phone  string
}

func main() {
	// Set the logger level to debug or based on your application's configuration
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(os.Stdout)
	//Set the days
	warning = 6
	overdue = 8
	//Open the Database
	db = opendb()
	//Search the Database for overdue SKUs
	skulookup()
}

func skulookup() {

	// Prepare the query
	query := `SELECT b.phone, a.sku, a.sorter, DATEDIFF(NOW(), a.checkout) AS days_since_checkout FROM purchasing.sortrequest a LEFT JOIN orders.users b ON a.sorter = b.username WHERE status = 'Checkout' AND DATEDIFF(NOW(), a.checkout) > 0 AND b.phone IS NOT NULL`
	log.WithFields(logrus.Fields{"query": query}).Debug("Executing query")

	// Run the DB Query
	rows, err := db.Query(query)
	if err != nil {
		log.WithFields(logrus.Fields{"error": err, "query": query}).Error("Error executing query")
		return // Early return on query execution failure
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.WithError(err).Error("Error closing rows")
		}
	}()

	// Initialize variables
	var sku, sorter, phone string
	var daysSinceCheckout int

	// Updated map structure
	users := make(map[SorterPhoneKey]map[string]int)

	for rows.Next() {
		err := rows.Scan(&phone, &sku, &sorter, &daysSinceCheckout)
		if err != nil {
			log.WithError(err).Error("Error scanning row")
			continue // Continue to the next row on error
		}

		// Debug log for each processed row
		log.WithFields(logrus.Fields{
			"sorter":            sorter,
			"phone":             phone,
			"sku":               sku,
			"daysSinceCheckout": daysSinceCheckout,
		}).Debug("Processed row")

		key := SorterPhoneKey{Sorter: sorter, Phone: phone}
		if _, ok := users[key]; !ok {
			users[key] = make(map[string]int)
		}
		if daysSinceCheckout >= warning {
			users[key][sku] = daysSinceCheckout
		}
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Error("Error iterating over rows")
	}

	// Generate messages for each user
	for key, skus := range users {
		if len(skus) > 0 {
			var skuList []string
			var daysSinceCheckout int
			for sku, days := range skus {
				daysSinceCheckout = days
				skuList = append(skuList, sku)
			}
			if len(skuList) > 0 {
				message := generateMessage(skuList, daysSinceCheckout)
				log.WithFields(logrus.Fields{
					"sorter":  key.Sorter,
					"phone":   key.Phone,
					"message": message,
				}).Info("Sending message")
				// sendsms(message, key.Phone)                           //Send to User
				// sendsms(key.Sorter+"-"+message, os.Getenv("MANAGER")) // Send to Manager
			}
		}
	}
}

func generateMessage(skuList []string, daysSinceCheckout int) string {
	var message string

	// For a single SKU
	if len(skuList) == 1 {
		sku := skuList[0]
		if daysSinceCheckout == warning {
			message = fmt.Sprintf("Just a friendly reminder that the following SKU is due back tomorrow: %s. Automated message from BBB Sorting. Please contact your manager if you have further questions.", sku)
		} else if daysSinceCheckout >= overdue {
			message = fmt.Sprintf("Just a friendly reminder that the following SKU is now overdue. Please return this SKU ASAP: %s. Automated message from BBB Sorting. Please contact your manager if you have further questions.", sku)
		}
	} else { // For multiple SKUs
		skusString := strings.Join(skuList, ", ")
		if daysSinceCheckout == warning {
			message = fmt.Sprintf("Just a friendly reminder that the following SKUs are due back tomorrow: %s. Automated message from BBB Sorting. Please contact your manager if you have further questions.", skusString)
		} else if daysSinceCheckout >= overdue {
			message = fmt.Sprintf("Just a friendly reminder that the following SKUs are now overdue. Please return these SKUs ASAP: %s. Automated message from BBB Sorting. Please contact your manager if you have further questions.", skusString)
		}
	}

	return message
}

func sendsms(message, toNumber string) {
	// Twilio Variables
	accountSid := os.Getenv("TWILIOSID")
	authToken := os.Getenv("TWILIOTOKEN")
	fromNumber := os.Getenv("TWILIONUMBER")

	// Create a new Twilio client with your credentials
	twilio := gotwilio.NewTwilioClient(accountSid, authToken)
	log.WithFields(logrus.Fields{
		"accountSid": accountSid,
		"authToken":  authToken,
	}).Debug("Created new Twilio client")

	// Send the SMS message
	_, exc, err := twilio.SendSMS(fromNumber, toNumber, message, "", "")
	log.WithFields(logrus.Fields{
		"from":    fromNumber,
		"to":      toNumber,
		"message": message,
	}).Debug("Sending SMS")

	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err,
			"from":  fromNumber,
			"to":    toNumber,
		}).Error("Error sending SMS")
		return
	}

	if exc != nil {
		log.WithFields(logrus.Fields{
			"TwilioException": exc.Message,
			"from":            fromNumber,
			"to":              toNumber,
		}).Error("Twilio exception")
		return
	}

	log.WithFields(logrus.Fields{
		"to": toNumber,
	}).Info("SMS sent successfully")
}

func opendb() (db *sql.DB) {
	var err error

	// Get a database handle.
	log.Info("Connecting to DB...")
	log.Debug("Opening Database...")
	connectstring := os.Getenv("USER") + ":" + os.Getenv("PASS") + "@tcp(" + os.Getenv("SERVER") + ":" + os.Getenv("PORT") + ")/purchasing?parseTime=true"
	log.Debug("Connection: ", connectstring)
	db, err = sql.Open("mysql",
		connectstring)
	if err != nil {
		log.Error("Message: ", err)
		return nil
	}

	//Test Connection
	pingErr := db.Ping()
	if pingErr != nil {
		log.Error("Message: ", pingErr)
		return nil
	}

	//Success!
	log.Info("Returning Open DB...")
	return db
}
