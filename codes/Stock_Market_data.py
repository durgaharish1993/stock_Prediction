# API KEY : UGJN261OWZA9KCVJ

import urllib.request
import urllib3
import json
import datetime,pytz

from  codes.Companies import constituent_data,ApiDetails,Company_data,stock_time_zone


# Thank you for the code

#
# class ApiDetails():
#     def __init__(self,api_key = 'UGJN261OWZA9KCVJ'):
#         self.api_key = api_key
#         self.source  ="Alpha Vantage"
#         self.source_link = "https://www.alphavantage.co/"
#
#
#     def intraDay_data_link(self, companySymbol, full_data=False):
#         '''
#         Link details Compact  : https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=1min&apikey=demo
#         Link Details Full     : https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=15min&outputsize=full&apikey=demo
#         '''
#         if not full_data:
#             link_address = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='+companySymbol+'&interval=1min&apikey='+ self.api_key
#         else:
#             link_address =  'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='+companySymbol+'&interval=15min&outputsize=full&apikey='+self.api_key
#
#         return link_address
#
#
#     def daily_data_link(self, companySymbol, full_data=False):
#         '''
#         Compact Data : https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&apikey=demo
#         Full Data    : https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&outputsize=full&apikey=demo
#         '''
#
#         if not full_data:
#             link_address = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=' + companySymbol + '&apikey=' + self.api_key
#         else:
#             link_address = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=' + companySymbol + '&outputsize=full&apikey=' + self.api_key
#
#         return link_address
#
#
#     # def get_data(self,companySymbol,full_data=False):
#     #
#     #     link = self.daily_data_link(companySymbol=companySymbol,full_data=full_data)
#     #
#
#dfdfddfdfd


def bulk_data_download():
    #############################################
    a = constituent_data()
    api_obj = ApiDetails()

    company_data = a.get_topK_dict(k=500)

    keys = company_data['Name'].keys()

    http = urllib3.PoolManager()
    today_date = datetime.datetime.today().strftime("%m%d%Y")

    raw_data = '/Users/dimbul/Desktop/temp_desktop/private/stock_Prediction/raw_data/json'
    ############################################

    for key in keys:
        print('################################################################################################')
        company_name    = company_data['Name'][key].replace(" ", "_")
        company_symbol  = company_data['Symbol'][key]
        file_name = raw_data + '/'+company_name + '_'+today_date+'_Stock_data.json'
        print('Getting Data for :',company_name)


        http_link = api_obj.daily_data_link(companySymbol=company_symbol,full_data=True)

        try:
            data_obj  = http.request('GET',http_link)
            json_data = json.loads(data_obj.data.decode('utf-8'))
            source_data = {'_Source':json_data}
            print("Writing Data of Company:",company_name)
            with open(file_name,'w') as fp:
                json.dump(source_data,fp)


        except:
            print('There is problem in fetching data:',company_name)









if __name__ == '__main__':
    bulk_data_download()