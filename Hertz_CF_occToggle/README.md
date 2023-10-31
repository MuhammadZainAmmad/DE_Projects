- **Project For**: Hertz Analytics 

- **Task Description**
    - fetch data from  Google Sheets (heirloom pricing model, tab: Compliance toggle), fetch data from BigQuery table (t_listing_dictionary), join both to get the listing id from BigQuery data
    - for each record (listing), find current time of its timezone, if time lies in range between values of time_in and time_out column write occ_in value in occupancy (accommodates) field of listing on Guesty via guesty api, else write occ_out value in occupancy