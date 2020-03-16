package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/buaazp/fasthttprouter"
	"github.com/hashicorp/vault/api"
	"github.com/patrickmn/go-cache"
	"github.com/valyala/fasthttp"
)

const pulsarURL string = "pulsar://localhost:6650"
const vaultURL string = "http://127.0.0.1:8200"
const vaultToken string = "s.Fs1kDhT9DJYhnS8qWr6oBVkq"
const cacheExpiry time.Duration = 1
const cachePurge time.Duration = 10

var gProducer pulsar.Producer
var gVaultClient *api.Client
var gCache *cache.Cache

// Health checks the status
func Health(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("up")
}

// ComputeHmac256 generates SHA256 HMAC
func ComputeHmac256(message []byte, secret string) string {
	key := []byte(secret)
	h := hmac.New(sha256.New, key)
	h.Write(message)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// GetAccessToken return access token for the reatilerMoniker
func GetAccessToken(retailerMoniker string) string {
	accessToken, found := gCache.Get(retailerMoniker)
	if found {
		log.Println("Found access token in cache")
		return accessToken.(string)
	}
	var keyName = "secret/magento/" + retailerMoniker
	secretValues, err := gVaultClient.Logical().Read(keyName)
	if (err == nil) && (secretValues.Data["accessToken"] != nil) {
		var accessToken = secretValues.Data["accessToken"].(string)
		gCache.Set(retailerMoniker, accessToken, cache.DefaultExpiration)
		return accessToken
	}
	return ""
}

// ProcessOrder process the incoming Orders
func ProcessOrder(ctx *fasthttp.RequestCtx) {

	var retailerMoniker = string(ctx.Request.Header.Peek("x-magento-retailer"))
	var hmacToken = string(ctx.Request.Header.Peek("x-magento-hmac-sha256"))

	log.Printf("Retailer: %v, hmacToken: %v", retailerMoniker, hmacToken)

	var accessToken = GetAccessToken(retailerMoniker)
	if accessToken == "" {
		fmt.Fprint(ctx, "Could not find access token for the retailer moniker")
	} else {
		var reqBody = ctx.PostBody()
		var generatedHMAC = ComputeHmac256(reqBody, accessToken)
		log.Printf("generatedHMAC: %v", generatedHMAC)

		if hmacToken == generatedHMAC {
			msg := pulsar.ProducerMessage{
				Payload: reqBody,
			}

			if err := gProducer.Send(context.Background(), msg); err != nil {
				fmt.Fprintf(ctx, "Producer could not send message: %v", err)
			} else {
				fmt.Fprint(ctx, "created")
			}
		} else {
			fmt.Fprint(ctx, "HMAC validation failed")
		}
	}
}

// InitializeVault initializes the vault connection
func InitializeVault() {
	vaultClient, err := api.NewClient(
		&api.Config{
			Address: vaultURL,
		},
	)

	if err != nil {
		log.Fatalf("Could not instantiate Vault client: %v", err)
	}

	log.Println("Connected to vault")

	vaultClient.SetToken(vaultToken)

	gVaultClient = vaultClient
}

// InitializePulsar initializes the pulsar connection
func InitializePulsar() {
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     pulsarURL,
		OperationTimeoutSeconds: 5,
		// MessageListenerThreads:  runtime.NumCPU(),
	})

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: "order",
	})

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar producer: %v", err)
	}

	log.Println("Connected to pulsar")

	// defer producer.Close()

	gProducer = producer
}

// InitializeCache initializes cache
func InitializeCache() {
	c := cache.New(cacheExpiry*time.Minute, cachePurge*time.Minute)
	gCache = c
}

func main() {

	InitializeVault()

	InitializePulsar()

	InitializeCache()

	// r := router.New()
	// r.GET("/", Index)
	// r.GET("/hello/:name", Hello)

	// log.Println("Server running on port 7070")
	// log.Fatal(fasthttp.ListenAndServe(":7070", r.Handler))

	router := fasthttprouter.New()
	router.GET("/health", Health)
	router.POST("/api/v1/order", ProcessOrder)

	log.Println("Server running on port 7070")
	log.Fatal(fasthttp.ListenAndServe(":7070", router.Handler))
}
