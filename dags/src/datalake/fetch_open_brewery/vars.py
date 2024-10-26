import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict

today = str(datetime.datetime.now(ZoneInfo("America/Sao_Paulo")).date())

endpoint = 'https://api.openbrewerydb.org/v1/breweries'
path = f"open_brewerydb_breweries/{today}/open_brewerydb_breweries.json"
bucket_name_dict = defaultdict(list)
tuple_data = (
    ('bronze','abinbev-open-brewery-bronze-datalake'),
    ('silver','abinbev-open-brewery-silver-datalake'),
    ('gold','abinbev-open-brewery-gold-datalake')
)

for key, value in tuple_data:
    bucket_name_dict[key].append(value)


bronze_bucket_name = bucket_name_dict['bronze'][0]
silver_bucket_name = bucket_name_dict['silver'][0]
s3_path = f"s3://{silver_bucket_name}/{path.split('/')[0]}"
partition_list = ['country','state_province']

origin_schema = bucket_name_dict['silver'][0].replace('-','_')
table_name = path.split('/')[0]
staging_dir = f"s3://{bucket_name_dict['silver'][0]}/"
query = f"""
        SELECT brewery_type, country, state_province, count(1) as qtd 
        from {origin_schema}.{table_name} 
        group by brewery_type, country, state_province 
        order by 4 desc, brewery_type, country, state_province"""

gold_bucket_name = bucket_name_dict['gold'][0]
gold_path = f"s3://{gold_bucket_name}/{table_name}_agg"