package producer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

// Define a struct for patient data.
type PatientData struct {
	PatientID   int    `json:"patient_id"`
	PatientName string `json:"patient_name"`
	Age         int    `json:"age"`
}

func ProducePatientData() {
	patients := []PatientData{
		{PatientID: 1, PatientName: "John Doe", Age: 30},
		{PatientID: 2, PatientName: "Jane Smith", Age: 25},
		{PatientID: 3, PatientName: "Bob Johnson", Age: 45},
	}

	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "demo-go-kafka", 0)
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	for _, patient := range patients {
		jsonData, err := json.Marshal(patient)
		if err != nil {
			// Proper error handling here
		}

		conn.WriteMessages(kafka.Message{Value: jsonData})
	}
}
