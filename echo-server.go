package main

import (
	"net/http"

	"github.com/labstack/echo"
)

func main1() {
	e := echo.New()
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, world")
	})
	e.Logger.Fatal(e.Start(":1323"))
}
