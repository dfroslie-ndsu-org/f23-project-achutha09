IF NOT EXISTS (SELECT schema_id FROM sys.schemas WHERE name = 'databaseSchema')
    EXEC('CREATE SCHEMA databaseSchema;');

DROP TABLE IF EXISTS databaseSchema.AirlineInformation;
DROP TABLE IF EXISTS databaseSchema.FlightInformation;
Drop SEQUENCE IF EXISTS databaseSchema.FlightInformationseq;
Drop SEQUENCE IF EXISTS databaseSchema.AirlineInformationseq;

CREATE SEQUENCE databaseSchema.FlightInformationseq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 100000
    START WITH 1;


CREATE SEQUENCE databaseSchema.AirlineInformationseq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 100000
    START WITH 1;



CREATE TABLE databaseSchema.FlightInformation (
                                                  id BIGINT PRIMARY KEY DEFAULT NEXT VALUE FOR databaseSchema.FlightInformationseq,
                                                  airportName VARCHAR(255),
                                                  airportCountryCode VARCHAR(10),
                                                  countryName VARCHAR(255),
                                                  airportContinent VARCHAR(255),
                                                  nationality VARCHAR(100),
                                                  continents VARCHAR(255),
                                                  departureDate DATE,
                                                  arrivalAirport VARCHAR(255),
                                                  flightStatus VARCHAR(50)
);



CREATE TABLE databaseSchema.AirlineInformation (
                                                   id BIGINT PRIMARY KEY DEFAULT NEXT VALUE FOR databaseSchema.AirlineInformationseq,
                                                   fleet_average_age FLOAT,
                                                   airline_id INT,
                                                   callsign VARCHAR(255),
                                                   hub_code VARCHAR(10),
                                                   iata_code VARCHAR(10),
                                                   icao_code VARCHAR(10),
                                                   country_iso2 VARCHAR(2),
                                                   iata_prefix_accounting VARCHAR(10),
                                                   airline_name VARCHAR(255),
                                                   country_name VARCHAR(255),
                                                   fleet_size INT,
                                                   status VARCHAR(50),
                                                   type VARCHAR(50),
                                                   journey_date DATE
);
