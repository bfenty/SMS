package main

import (
	"database/sql"
	"fmt"
	"strings"

	"os"

	"github.com/sfreiberg/gotwilio"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// Initialize the DB Variable
var db *sql.DB

func main() {
	//Open the Database
	db = opendb()
	skulookup()
}

func skulookup() {
	// Prepare the query
	query := `SELECT b.phone, a.sku, a.sorter, DATEDIFF(NOW(), a.checkout) as days_since_checkout FROM purchasing.sortrequest a left join orders.users b on a.sorter = b.username WHERE checkout <= DATE_SUB(NOW(), INTERVAL 6 DAY) AND checkint IS NULL`

	// Debug log the query being executed
	log.Printf("Executing query: %s", query)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()

	var sku, sorter, phone string
	var daysSinceCheckout int

	// Group the results by user
	users := make(map[string]map[string]int)
	for rows.Next() {
		err := rows.Scan(&phone, &sku, &sorter, &daysSinceCheckout)
		if err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		if _, ok := users[sorter]; !ok {
			users[sorter] = make(map[string]int)
		}
		if daysSinceCheckout == 6 {
			users[sorter][fmt.Sprintf("%s", sku)] = daysSinceCheckout
		} else if daysSinceCheckout >= 9 {
			users[sorter][fmt.Sprintf("%s", sku)] = daysSinceCheckout
		} else if daysSinceCheckout >= 6 {
			users[sorter][fmt.Sprintf("%s", sku)] = daysSinceCheckout
		}
		log.Debug(users)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating over rows: %v", err)
	}

	// Generate messages for each user
	for user, skus := range users {
		// Check if the user has any SKUs that meet the criteria
		if len(skus) > 0 {
			var message string
			var skuList []string
			var daysSinceCheckout int
			// Loop over the SKUs for the user
			for sku, days := range skus {
				// Update the days since checkout to the latest value
				daysSinceCheckout = days
				// Add the SKU to the list
				skuList = append(skuList, sku)
			}
			// Check if there are any SKUs for the user
			if len(skuList) > 0 {
				// Generate the message based on the number of SKUs and the days since checkout
				if len(skuList) == 1 {
					// For a single SKU
					log.Debugf("Days since checkout for SKU %s: %d", skuList[0], daysSinceCheckout)
					if daysSinceCheckout == 6 {
						message = fmt.Sprintf("Automated message from BBB Sorting. Please contact your manager if you have further questions. Just a friendly reminder that the following SKU is due back tomorrow: %s", skuList[0])
					} else if daysSinceCheckout >= 9 {
						message = fmt.Sprintf("Automated message from BBB Sorting. Please contact your manager if you have further questions. Just a friendly reminder that the following SKU is now overdue. Please return this SKU ASAP: %s", skuList[0])
					} else {
						message = fmt.Sprintf("Automated message from BBB Sorting. Please contact your manager if you have further questions. Just a friendly reminder that the following SKU is due back soon: %s", skuList[0])
					}
				} else {
					// For multiple SKUs
					log.Debugf("Days since checkout for SKUs %s: %d", strings.Join(skuList, ", "), daysSinceCheckout)
					if daysSinceCheckout == 6 {
						message = fmt.Sprintf("Automated message from BBB Sorting. Please contact your manager if you have further questions. Just a friendly reminder that the following SKUs are due back tomorrow: %s", strings.Join(skuList, ", "))
					} else if daysSinceCheckout >= 9 {
						message = fmt.Sprintf("Automated message from BBB Sorting. Please contact your manager if you have further questions. Just a friendly reminder that the following SKUs are now overdue. Please return these SKUs ASAP: %s", strings.Join(skuList, ", "))
					} else {
						message = fmt.Sprintf("Automated message from BBB Sorting. Please contact your manager if you have further questions. Just a friendly reminder that the following SKUs are due back soon: %s", strings.Join(skuList, ", "))
					}
				}
				// Print the message for debugging purposes (replace with code to send the message to the user)
				fmt.Printf("Sending message to user %s:\n%s\n Phone: %s", user, message, phone)
				// send message to user
				sendsms(message, phone)
			}
		}
	}
}

func sendsms(message string, toNumber string) {

	//Twilio Variables
	accountSid := os.Getenv("TWILIOSID")
	authToken := os.Getenv("TWILIOTOKEN")
	fromNumber := os.Getenv("TWILIONUMBER")

	// Create a new Twilio client with your credentials
	twilio := gotwilio.NewTwilioClient(accountSid, authToken)

	// Send the SMS message
	_, exc, err := twilio.SendSMS(fromNumber, toNumber, message, "", "")
	if err != nil {
		log.Fatal(err)
	}

	//Error Handling
	if exc != nil {
		log.Fatal(exc.Message)
	}

	log.Info("SMS sent successfully!")
}

func opendb() (db *sql.DB) {
	var err error
	user := os.Getenv("USER")
	pass := os.Getenv("PASS")
	server := os.Getenv("SERVER")
	port := os.Getenv("PORT")
	// Get a database handle.
	log.Info("Connecting to DB...")
	log.Debug("user:", user)
	log.Debug("pass:", pass)
	log.Debug("server:", server)
	log.Debug("port:", port)
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
