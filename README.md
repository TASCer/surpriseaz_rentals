# hoa-insights
"HELPING KEEP HOA BOARDS IN THE KNOW AND COMMUNITY MANAGEMENT COMPANIES HONEST"

Provides insights into a HOA community by accessing public data and presenting it in a timely and board member friendly 
manner.

PDF report functionality will need the pdfkit Python module and wkhtmltopdf to render. https://wkhtmltopdf.org/



Currently, the county assessor's office is the only entity providing a free API that I could find. 
    
POSSIBLE OTHER APIS:

    1 - Recorder's office would be a benefit.

    2 - Criminalality - crimometer/Google (7 day demo, pricing?), Crime Dats free

    3 - Community Vendors - Legal, Landscape, Collections

Provides insights on:

- Community Rentals
- Community Sales - 161 missing sales price and date out of 515?! Researching. Seems they zero out
when there is a deed change or very slow to update from??
Zillow has the data with a public source...
- 
- Community Foreclosure - ?? TBD as I cannot find this flag coming from mcassessor yet
- Property Deed History - deed/chain is not up as of 7/29/2022

- Community Interactive Map
  -Shows rental properties and contact information for propery management employees  

API Documentation: https://mcassessor.maricopa.gov/file/home/MC-Assessor-API-Documentation.pdf

Per Assessor Office: All API data is refreshed at 5:00 AM (Arizona time), Monday-Friday.

There is usually 10-15 minutes of downtime from 6:00AM to 6:15AM (Arizona time) while we refresh our indexes.

API's used: deeds = deeds/chain/apn ?? TBD if available
            parcel = parcel/{apn}
            
issues with rental status veracity, sales info, foreclosure flag,  and chained deeds