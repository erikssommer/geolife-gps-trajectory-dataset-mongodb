# geolife-gps-trajectory-dataset-mongodb
## TDT4225 - Very Large, Distributed Data Volumes

### Group: 99

### Database: MongoDB

### Dataset
Modified dataset based on Geolife GPS Trajectory dataset

> The dataset should be located in the root of the project folder and be named 'dataset'

### Setup
```bash
pip3 install -r requirements.txt
```

### Build and run MongoDB database
```bash
# Navigate to the docker folder from root
cd docker
# Build the docker image
docker-compose up --build -V
```

### Run the script in seperate shell
Initialize the database with data and run queries
```bash
# Navigate to the src folder from root
cd src 
# Run the script with flag -i to initialize the database
python3 main.py -i
```

Run only queries, requires database to be initialized
```bash
# Navigate to the src folder from root
cd src
# Run the script without flag
python3 main.py
```

### Docker prune
Prune volumes if running docker-compose build command multiple times
```bash
docker volume prune
```
