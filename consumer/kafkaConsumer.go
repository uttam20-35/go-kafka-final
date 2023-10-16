package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Define a struct for patient data.
type PatientData struct {
	PatientID   int    `json:"patient_id"`
	PatientName string `json:"patient_name"`
	Age         int    `json:"age"`
}

func ConsumePatientData() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "demo-go-kafka", // Adjust the topic name as needed
		GroupID:  "my-group",
		MaxBytes: 10e6, // 10MB, adjust as needed
	})

	defer r.Close()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		message, err := r.ReadMessage(ctx)
		if err != nil {
			if err == context.DeadlineExceeded {
				fmt.Println("Context deadline exceeded. Exiting...")
				cancel()
				break
			} else {
				// Proper error handling here
			}
		} else {
			var patient PatientData
			if err := json.Unmarshal(message.Value, &patient); err != nil {
				fmt.Printf("Error unmarshalling JSON: %v\n", err)
			} else {
				// Process the patient data
				fmt.Printf("Received patient data: %+v\n", patient)
			}
		}
	}
}
