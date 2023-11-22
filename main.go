package main

import (
	"database/sql"
	"fmt"
	"strings"

	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sfreiberg/gotwilio"
	log "github.com/sirupsen/logrus"
)

// Initialize the DB Variable
var db *sql.DB

//initialize other variables
var warning int
var overdue int

// Define a struct for the map key
type SorterPhoneKey struct {
	Sorter string
	Phone  string
}

func main() {
	//Set the logging level
	log.SetLevel(log.InfoLevel)
	log.SetOutput(os.Stdout)
	//Set the days
	warning=6
	overdue=8
	//Open the Database
	db = opendb()
	//Search the Database for overdue SKUs
	skulookup()
}

func skulookup() {
	// Prepare the query
	query := `SELECT b.phone, a.sku, a.sorter, DATEDIFF(NOW(), a.checkout) as days_since_checkout FROM purchasing.sortrequest a left join orders.users b on a.sorter = b.username WHERE status = 'Checkout' AND DATEDIFF(NOW(), a.checkout) > 0 and b.phone is not null`

	// Debug log the query being executed
	log.Debugf("Executing query: %s", query)

	//Run the DB Query
	rows, err := db.Query(query)
	if err != nil {
		log.Errorf("Error executing query: %v", err)
	}
	defer rows.Close()

	//Initialize variables
	var sku, sorter, phone string
	var daysSinceCheckout int

	// Updated map structure
	users := make(map[SorterPhoneKey]map[string]int)

	for rows.Next() {
		err := rows.Scan(&phone, &sku, &sorter, &daysSinceCheckout)
		if err != nil {
			log.Errorf("Error scanning row: %v", err)
			continue  // Continue to the next row on error
		}

		//Debug log for each processed row
		log.Debugf("Processed: Sorter: %s, Phone: %s, SKU: %s, DaysSinceCheckout: %d", sorter, phone, sku, daysSinceCheckout)

		key := SorterPhoneKey{Sorter: sorter, Phone: phone} //Key is a combination of sorter and phone number
		if _, ok := users[key]; !ok {
			users[key] = make(map[string]int)
		}
		if daysSinceCheckout >= warning {
			users[key][sku] = daysSinceCheckout //if it's been longer than the warning period, add to the map
		}
	}
	if err := rows.Err(); err != nil {
		log.Errorf("Error iterating over rows: %v", err)
	}

	// Generate messages for each user
	for key, skus := range users {
		// Check if the user has any SKUs that meet the criteria
		log.Debugf("user:%s Phone:%s skus:%d",key.Sorter,key.Phone,len(skus))
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
				log.Debugf("Generating message for %s",key.Sorter)
				message = generateMessage(skuList, daysSinceCheckout)
				// Print the message for logging purposes
				log.Info("Sending message to Sorter: %s, Phone: %s:\n%s\n", key.Sorter, key.Phone, message)
				// Send message to user
				if message != "" {
					sendsms(message, key.Phone) //Send to User
					sendsms(key.Sorter+"-"+message, "9314349554")  // Send to Manager
				}
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


func sendsms(message string, toNumber string) {

	//Twilio Variables
	accountSid := os.Getenv("TWILIOSID")
	authToken := os.Getenv("TWILIOTOKEN")
	fromNumber := os.Getenv("TWILIONUMBER")

	// Create a new Twilio client with your credentials
	twilio := gotwilio.NewTwilioClient(accountSid, authToken)
	log.Debug("accoountsid:", accountSid, " auth:", authToken)

	// Send the SMS message
	_, exc, err := twilio.SendSMS(fromNumber, toNumber, message, "", "")
	log.Debug("FROM:", fromNumber, " TO:", toNumber, " MESSAGE:", message)
	if err != nil {
		log.Error(err)
	}

	// Error Handling
	if exc != nil {
		log.Errorf(exc.Message)
	}

	log.Info("SMS sent successfully!")
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
