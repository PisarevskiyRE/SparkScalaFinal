package com.example
package schemas

case class Airport(
                    IATA_CODE: String,
                    AIRPORT: String,
                    CITY: String,
                    STATE: String,
                    COUNTRY: String,
                    LATITUDE: Double,
                    LONGITUDE: Double
                  )
