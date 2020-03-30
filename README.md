# VehicleAccident

This application to analyze a motor vehicle collisions (crashes) per cities for the period from 01-07-2012 to 15-03-2020 inclusive.


### Prerequisites

To running application is used:
python version 3.7.4.
pip version 20.0.2.
Docker Desktop (https://www.docker.com/products/docker-desktop) according to your current os (during development was using Docker Desktop version 2.2.0.4).
npm (https://nodejs.org/en/download/ for Windows) according to your current os (during development was using npm version 6.13.4). For Linux in terminal execute the command 'sudo apt install npm'.
postgresql (https://www.postgresql.org/) if you want to use the database directly via pgadmin.

Also you need:
- create own virtual environment (command in terminal in project root directory 'python -m venv env)';
- install requirements execute the command in terminal 'pip install -r requirements.txt' (or for Linux 'sudo pip install -r requirements.txt').


## Getting Started

Before start you need to get access to database vehicleaccidents: send your IP to database administrator Orlovskaya Anastasia by e-mail: nastassia.orlovskaya@gmail.com or skype: tasenga (you can look for own IP via service https://yandex.by/internet/, if you don't know IP).
If you want to use the database directly via pgadmin, ask database administrator about configuration parameters for access to the server.

Then, for running project do following steps:
1. Get a clone of the current repository (https://github.com/Tasenga/VehicleAccident) on your machine, if this has not been done before.
2. To install a local server using npm:
2.1. in PowerShell (for Windows) move to the project directory .\VehicleAccident\third_party\geo_data and execute the command 'npm install http-server',
in terminal (for Linux) move to the project directory ./VehicleAccident/third_party/geo_data and execute the command 'sudo npm install http-server';
2.2. download geojson to the same directory from https://github.com/codeforamerica/click_that_hood/blob/master/public/data/new-york-city-boroughs.geojson (click 'download' and save the open data);
2.3. in PowerShell (for Windows) and in terminal (for Linux) in the same directory execute the command 'http-server --cors='kbn-version' -p 8000',
(!!! NOTE for Windows you might need to configure the execution policy in advance: execute the command 'Set-ExecutionPolicy -Scope CurrentUser RemoteSigned').
2.4. copy the available host starting with 'http://192....' from PowerShell (or terminal) and modify the file .\VehicleAccident\third_party\elk\kibana\config\kibana.yml by updating host on line 10.
3.  run the elk docker from the project directory .\VehicleAccident\third_party\geo_data and execute the command 'docker-compose up --build' and wait the log message 'elk_logstash exited with code 0'.
4. open Kibana in your internet browser via link http://localhost:5601.

To create Region map of New York vehicle accidents per boroughs:
5. create new index (write 'ny_borough' into the index pattern field on http://localhost:5601/app/kibana#/management/kibana/index_pattern?_g=() and follow the advices of Kibana).
6. create new Region map for index pattern 'ny_borough' (http://localhost:5601/app/kibana#/visualize?_g=()).
7. adjust the following settings: Data->Metrics->Aggregation=Max and Field=count, Data->Buckets->Aggregation=Terms and Field=borough, Options->Layer Settings=Borough NYC and Join field=NYC Borough.
8. Save Region map with name "NYC Boroughs"

To create Region map of New York vehicle accidents per neighborhoods:
9. create new index (write 'ny_neighborhoods' into the index pattern field on http://localhost:5601/app/kibana#/management/kibana/index_pattern?_g=() and follow the advices of Kibana).
10. create new Region map for index pattern 'ny_neighborhoods' (http://localhost:5601/app/kibana#/visualize?_g=()).
11. adjust the following settings: Data->Metrics->Aggregation=Max and Field=count, Data->Buckets->Aggregation=Terms, Field=neighborhood and Size=300, Options->Layer Settings=Neighborhoods NYC and Join field=neighborhood.
12. Save Region map with name "NYC Neighborhoods"

To create interactive controls for easy dashbord manipulations:
13. create new Controls in Create Visualization menu.
14. adjust the following settings: Controls ->choose Options list and click +Add button, Enter name for ex. 'Neighborhood' in Control Label, Index Pattern='ny_neighborhoods', Field=neighborhood.keyword. At the next block parametr choose Range slidebar and click +Add button, Enter name for ex. 'Number of accidents' in Control Label, Index Pattern='ny_neighborhoods', Field=count
15. Apply changes and save Control with name "Conrols Visualization"

To create Dashbord:
16. create new dashbord in Dashbords menu. Click add and choose from a list next panels:"NYC Boroughs", "NYC Neighborhoods" and "Conrols Visualization".
17. Save created Dashbord with any name.


## Running the tests

don't ready ```
```unit tests were created with using python module "pytest".
```Tests run from appropriate module after command in command line "python -m pytest discover -t ..".
```All tests should finish successful to push module changing to external repository.

## Additional materials

To get the configuration parameters to get the access to the database ask the administrator database - Orlovskaya Anastasia.
To get the raw data, ask the developers.

## Versioning

Version history:
2020.03.23 - v.1.0.0. - current version

## Authors

Bykov Sergey - (e_mail: mr.tuniguk@gmail.com)
Dovger Ivan - (e_mail: 1ivan.dovger@mail.ru)
Kirychenko Andrey - (e_mail: akvaby@gmail.com)
Orlovskaya Anastasia - (e_mail: nastassia.orlovskaya@gmail.com)
Skiba Alexandra - (e_mail: debrikosar@gmail.com)
