set -e

TAXI_TYPE=$1 #'yellow'
YEAR=$2 #2021

#https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv

URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

for MONTH in {1..12}; do
   FMONTH=`printf "%02d" ${MONTH}`
   URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
   LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
   LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"
   LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

   mkdir -p ${LOCAL_PREFIX}

   echo "downloding ${URL} to ${LOCAL_PATH}"
   wget ${URL} -O ${LOCAL_PATH}
   
   echo "compressing ${LOCAL_file}"
   gzip ${LOCAL_PATH}
   
done