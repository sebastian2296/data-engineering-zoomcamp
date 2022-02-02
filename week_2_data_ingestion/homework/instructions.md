# Preparing data for next week

We'll be creating three DAGs for this week homework:

    * One for transfering the yellow taxi-data (2019-2020) -> Create template

        * First period link 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv'

    * One for transfering the FHV (data)

        * https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2021-01.csv

    * One for transfering the zones data

        * https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv


This will result in one file containing common functions (parquetize and upload to GCS) and another three files containing DAGs for each one of the steps described above.