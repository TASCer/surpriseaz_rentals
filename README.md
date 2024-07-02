![TASCS LOGO](./hoa_insights/images/logo.png)
# surpriseaz_rentals
"HELPING KEEP HOA BOARDS IN THE KNOW AND COMMUNITY MANAGEMENT COMPANIES HONEST"

Provides rental parcel information for a Home Owners Association (HOA) community by accessing public data and presenting it in a timely, community member friendly, and accessible manner.

### See It: 
[Rental Map of area](https://hoa.tascs.net/areaMap.php)

[Relevant HOA Legislation](https://hoa.tascs.test/relevant_bills.php)

Currently, the county assessor's office and Legiscan are the only entities providing a free API that I could find. 
    
POSSIBLE OTHER APIS:

    1 - County Recorder's Office

    2 - Crime Stats

    3 - Community Vendors 
        a. Management Company
        b. Legal
        c. Landscape
        d. Collections

Provides insights on:

- Community Rentals
    - Property Contact Information
    - Property Owner Information
    - Property Mapped Location

- Community Sales
- Legislation information on relevant HOA bills

Assessor API Documentation: https://mcassessor.maricopa.gov/file/home/MC-Assessor-API-Documentation.pdf

Legiscan API Information: https://legiscan.com/legiscan

PDF report functionality will need the pdfkit Python module and wkhtmltopdf to render. https://wkhtmltopdf.org/

#### utils folder contains:

* Linux shell script for cron job scheduling
* Windows batch file for Scheduled Tasks scheduling
* A template of my 'my_secrets.py' secrets file

#### PRE_LAUNCH TODO's

* [ ] TASC 1 - CREATE 'input' dir and 'zipped_logfiles' subdir in root dir
* [ ] TASC 2 - CREATE 'output' dir and 'unzipped_logfiles' subdir in root dir

