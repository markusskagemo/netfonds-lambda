#### AWS Lambda cron-job: Oppdatering av datalagre  
![alt text](https://github.com/saturdayquant/netfonds-lambda/blob/master/assets/kvant_lambda.png "Cron job programflyt")  
AWS Lambda kjører calls til Netfonds og legger til dataene i driven vår med Google Sheets API.

#### Filbeskrivelser  
* *kvant_google_api.py*: Har med autentisering av Google APIer, og ellers hvordan man lister filer, skriver til sheets, og får celleinfo.  
* *touch.py*: Overordnet program, lambda_function er en utvidelse av denne.
* *netfonds_utils.py*: Hjelpsomme funksjoner for Netfonds-relaterte ting.  
* *populate_all_headers.py*: Program som gir alle filer i en drive-mappe passende headere. F.eks. "time, bid, ask, ...". 