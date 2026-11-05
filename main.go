package main

import (
	"fly-together/backend/internal/services"
	"log"
	"net/http"
	"os"
)




func main() {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "."
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "3001"
	}
	a, err := services.NewApp(dataDir)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Fly Together Go API listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, a.Handler()))
}
