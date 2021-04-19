package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"net/http"
	"strings"
)

const (
	topic         = "testtp1"
	brokerAddress = "localhost:9092"
)

type GeoData struct {
	vehicleId 	string `json:"vehicle_id"`
	distance 	string `json:"distance"`
	xCoordinate string `json:"x_coordinate"`
	yCoordinate string `json:"y_coordinate"`
}

func consume(ctx context.Context) {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "my-group",
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}

		msgs := string(msg.Value)
		fmt.Println("received: ", msgs)
		slice := strings.Split(msgs, ",")
		geoDatas := map[string]string{"vehicle_id": slice[0], "distance": slice[1], "x_coordinate": slice[2], "y_coordinate": slice[3]}
		pbytes, _ := json.Marshal(geoDatas)
		buff := bytes.NewBuffer(pbytes)
		resp, err := http.Post("http://localhost:8080/geospatial/insert", "application/json", buff)

		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
	}
}

func main(){
	ctx := context.Background()
	consume(ctx)
}