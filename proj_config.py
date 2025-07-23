db_path = "wcmkt2.db" #path to your local market data database
wcmkt_url = f"sqlite+libsql:///{db_path}" #url of your local market data database
sde_path = "sde_info.db" #path to your local SDE database
sde_url = f"sqlite+libsql:///{sde_path}" #url of your local SDE database
fittings_path = "wcfitting.db"
fittings_url = f"sqlite+libsql:///{fittings_path}"

google_private_key_file = "wcdoctrines-1f629d861c2f.json" #name of your google service account key file
google_sheet_url = "https://docs.google.com/spreadsheets/d/1frGs3XzB7kooVoN-rqRUfoYX3k3FIFgo1ZDAypzc-pI/edit?gid=1738061156#gid=1738061156" #url of your google sheet
sheet_name = "nakah_market_data" #name of the sheet you want to update

sys_id = 30000072
reg_id = 10000001

user_agent = 'wcmkts_backend/1.0, orthel.toralen@gmail.com, (https://github.com/OrthelT/wcmkts_backend)'