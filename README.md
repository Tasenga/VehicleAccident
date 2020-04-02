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
!!! NOTE for Windows you might need to use wheel to install requirements package rasterio and GDAL.
    Check your system parameters and get suitable wheel via link https://www.lfd.uci.edu/~gohlke/pythonlibs/#rasterio.


## Getting Started

Before start you need to get access to database vehicleaccidents: send your IP to database administrator Orlovskaya Anastasia by e-mail: nastassia.orlovskaya@gmail.com or skype: tasenga (you can look for own IP via service https://yandex.by/internet/, if you don't know IP).
If you want to use the database directly via pgadmin, ask database administrator about configuration parameters for access to the server.

Then, for running project do following steps:
1. Get a clone of the current repository (https://github.com/Tasenga/VehicleAccident) on your machine, if this has not been done before.
2. To install a local server using npm:
2.1. in PowerShell (for Windows) move to the project directory .\VehicleAccident\third_party\geo_data and execute the command 'npm install http-server',
in terminal (for Linux) move to the project directory ./VehicleAccident/third_party/geo_data and execute the command 'sudo npm install http-server';
2.2. download geojson about borders by the links indicated in additional materials to the same directory 2.1. (save the open data "ctrl+s");
2.3. in PowerShell (for Windows) and in terminal (for Linux) in the same directory execute the command 'http-server --cors='kbn-version' -p 8000',
(!!! NOTE for Windows you might need to configure the execution policy in advance: execute the command 'Set-ExecutionPolicy -Scope CurrentUser RemoteSigned').
2.4. copy the available host starting with 'http://192....' from PowerShell (or terminal) and modify the file .\VehicleAccident\third_party\elk\kibana\config\kibana.yml by updating url.
3.  run the elk docker from the project directory .\VehicleAccident\third_party\geo_data and execute the command 'docker-compose up --build' and wait the log message 'elk_logstash exited with code 0'.
4. open Kibana in your internet browser via link http://localhost:5601.

###### To create Region map of New York vehicle accidents per boroughs:

5. create new index (write 'ny_borough' into the index pattern field on http://localhost:5601/app/kibana#/management/kibana/index_pattern?_g=() and follow the advices of Kibana).
6. create new Region map for index pattern 'boroughs' (http://localhost:5601/app/kibana#/visualize?_g=()).
7. adjust the following settings: Data->Metrics->Aggregation=Max and Field=count, Data->Buckets->Aggregation=Terms and Field=borough, Options->Layer Settings=Borough NYC and Join field=NYC boroughs.
8. Save Region map with name "NYC Boroughs"

###### To create Region map of New York vehicle accidents per neighborhoods:

9. create new index (write 'neighborhoods' into the index pattern field on http://localhost:5601/app/kibana#/management/kibana/index_pattern?_g=() and follow the advices of Kibana).
10. create new Region map for index pattern 'neighborhoods' (http://localhost:5601/app/kibana#/visualize?_g=()).
11. adjust the following settings: Data->Metrics->Aggregation=Max and Field=count, Data->Buckets->Aggregation=Terms, Field=neighborhood and Size=300, Options->Layer Settings=Neighborhoods NYC and Join field=NYC neighborhoods.
12. Save Region map with name "NYC Neighborhoods"

###### To create interactive controls for easy dashboard manipulations:

13. create new Controls in Create Visualization menu.
14. adjust the following settings: Controls ->choose Options list and click +Add button, Enter name for ex. 'Neighborhood' in Control Label, Index Pattern='neighborhoods', Field=neighborhood.keyword. At the next block parameter choose Range sidebar and click +Add button, Enter name for ex. 'Number of accidents' in Control Label, Index Pattern='neighborhoods', Field=count. At the next block parameter choose Range sidebar and click +Add button, Enter name for ex. 'Dangerous Index, where 1 is the least dangerous' in Control Label, Index Pattern='dangerous', Field=dangerous.
15. Apply changes and save Control with name "Controls Visualization"

###### To create Vertical Bar of the impact of weather on the accident rate in New York:

16. create new index (write 'weather' into the index pattern field on http://localhost:5601/app/kibana#/management/kibana/index_pattern?_g=() and follow the advices of Kibana).
17. create new Vertical Bar for index pattern 'weather' (http://localhost:5601/app/kibana#/visualize?_g=()).
18. adjust the following settings: Data->Metrics->Aggregation=Max and Field=accidents_per_hour and Custom Label=accidents_per_hour, Data->Buckets->Aggregation=Terms, Field=weather.keyword and Size=14 and Custom Label=type_of_weather.
19. Save Vertical Bar with name "The impact of weather on the accident rate in New York"

###### To create Dashboard:

20. create new dashboard in Dashboards menu. Click add and choose from a list next panels:

- "NYC Boroughs",
- "NYC Neighborhoods"
- "Controls Visualization"
- "The impact of weather on the accident rate in New York"
21. Save created Dashboard with any name.


## Running the tests

don't ready ```
```unit tests were created with using python module "pytest".
​```Tests run from appropriate module after command in command line "python -m pytest discover -t ..".
​```All tests should finish successful to push module changing to external repository.


## Additional materials

The data about borders of boroughs in New York: https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/new-york-city-boroughs.geojson
The data about borders of neighborhoods in New York: https://raw.githubusercontent.com/HodgesWardElliott/custom-nyc-neighborhoods/master/custom-pedia-cities-nyc-Mar2018.geojson
The data about population in New York: https://data.cityofnewyork.us/City-Government/New-York-City-Population-By-Neighborhood-Tabulatio/swpk-hqdp
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

```