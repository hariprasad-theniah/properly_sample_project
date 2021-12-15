# properly_sample_project
This project read data from a API and a local file. Transforms the data and loads into a POSTGRESQL database. Its a containarized application. To run this application you need a DOCKER environment

Please follow below step to run the project.

Create a new directory, and clone this repository into the directory. This repository has .env file required for postgres environment

change your cuurent directory to the new directory

cd ./{new directory name}

The Local IP address need to be modified, to connect to the POSTGRESQL from with in the DOCKER scripts.

vi .env

change the POSTGRES_HOST, value in the file, There should not be any blank spaces between the = operator

POSTGRES_HOST={Your Systems IP Address}

save and exit the vi editor
  
setting up the environment
  
step : 1 docker pull postgres
  
step : 2 docker run -d --name properly-db --env-file='./.env' -p 5432:5432 postgres
  
step : 3 ./build_mydocker
  
step : 4 docker run -t --env-file='./.env' properly-v0.01 run_pipeline.py
