import numpy as np
import pandas as pd
from matplotlib.pyplot import figure
import matplotlib.pyplot as plt
import seaborn as sns
import json
import websocket, time
from datetime import datetime,timedelta,timezone
import rel
long_period = 20 # 20 min window of the data in the chart
short_period = 5 # 5 min time to redraw the graph
pause_time = 0.1 # At the slow PC need increase time



def create_Alltrades_frame(trades):
    
    frame = pd.DataFrame(trades) \
        .assign(timestamp = lambda trade: pd.to_datetime(trade.timestamp,format="%Y-%m-%dT%H:%M:%S.%fZ"),
                price = lambda trade: pd.to_numeric(trade.price))
    return frame

class Liq:
    def __init__(self):
        self.socket = "wss://www.bitmex.com/realtime?subscribe=orderBookL2_25:XBTUSD"
        self.ws = websocket.WebSocketApp(self.socket, 
                                         on_open=self.on_open,
                                         on_message=self.on_message, 
                                         on_error=self.on_error, 
                                         on_close=self.on_close)

        self.last_minute = datetime.now().minute
        self.order_buy =[]
        self.order_sell =[]
        
        self.all_sell =[]
        self.all_buy =[]
         
        self.data_sell = pd.DataFrame()
        self.data_buy = pd.DataFrame()
        

    def on_message(self,ws, message):
        x = json.loads(message)

        if x['action'] == 'insert':

            if x['data'][0]['side'] =='Sell':  
                self.order_sell.append(x['data'][0])
            elif x['data'][0]['side'] =='Buy': 
                self.order_buy.append(x['data'][0])
        
            if len(x['data'])>1:
                if x['data'][1]['side'] =='Sell':  
                    self.order_sell.append(x['data'][1])
                elif x['data'][1]['side'] =='Buy': 
                    self.order_buy.append(x['data'][1])

            
          
        now =  datetime.now(timezone.utc).replace(tzinfo=None)

        if (now.minute ==0 or now.minute % long_period ==0) and self.last_minute != now.minute:

            plt.close()
            plt.pause(pause_time)    #At        
            
            self.last_minute = now.minute
            self.all_sell = self.all_sell + self.order_sell
            self.all_buy = self.all_buy + self.order_buy                
            

            
            data_sell = create_Alltrades_frame(self.all_sell)
            data_sell['size']= data_sell['size'].values.astype(np.int64) 
            data_buy = create_Alltrades_frame(self.all_buy)
            data_buy['size']= data_buy['size'].values.astype(np.int64)

            
            big_buy = data_buy[(data_buy['size']>=15000)&(data_buy['size']<50000)]
            bigS_buy = data_buy[(data_buy['size']>=50000)&(data_buy['size']<150000)]
            bigG_buy = data_buy[data_buy['size']>=150000]
     
            big_sell = data_sell[(data_sell['size']>=15000 )&(data_sell['size']<50000)]
            bigS_sell = data_sell[(data_sell['size']>=50000)&(data_sell['size']<150000)]
            bigG_sell = data_sell[data_sell['size']>=150000]
            
       
            n = 10 # count of the top trade
            #trade from 15 000 to 50 000
            mostsell = big_sell['size'].value_counts().index.tolist()[:n]
            bigsell_show =  big_sell[big_sell['size'].isin(mostsell)]
            #
            mostsbuy = big_buy['size'].value_counts().index.tolist()[:n]
            bigbuy_show =  big_buy[big_buy['size'].isin(mostsbuy)]
            
            #trade from 50 000 to 150 000
            mostsellS = bigS_sell['size'].value_counts().index.tolist()[:n]
            bigsellS_show =  bigS_sell[bigS_sell['size'].isin(mostsellS)]
            #
            mostsbuyS = bigS_buy['size'].value_counts().index.tolist()[:n]
            bigbuyS_show =  bigS_buy[bigS_buy['size'].isin(mostsbuyS)]
            
            mostsellG = bigG_sell['size'].value_counts().index.tolist()[:3]
            bigsellG_show =  bigG_sell[bigG_sell['size'].isin(mostsellG)]
            #
            mostsbuyG = bigG_buy['size'].value_counts().index.tolist()[:3]
            bigbuyG_show =  bigG_buy[bigG_buy['size'].isin(mostsbuyG)]
            
            figure(figsize=(14,4), dpi=80)
            sns.scatterplot(x="timestamp", y="price",color = 'mediumseagreen', data=bigsell_show,s=10)#,marker="+"
            sns.scatterplot(x="timestamp", y="price",color = 'green',  data=bigsellS_show,s=10)
            sns.scatterplot(x="timestamp", y="price",  color='darkslategray' ,data=bigsellG_show,s=120) 
            
            sns.scatterplot(x="timestamp", y="price", color='coral', data=bigbuy_show,s=10)#marker="+",
            sns.scatterplot(x="timestamp", y="price",  color='indianred' ,data=bigbuyS_show,s=10)       
            sns.scatterplot(x="timestamp", y="price",  color='darkred' ,data=bigbuyG_show,s=120)  
            
            plt.draw()
            plt.pause(pause_time)
            
            
            self.data_sell = data_sell.copy()
            self.data_buy = data_buy.copy()            

            
            self.order_buy =[]
            self.order_sell =[]    
            self.all_sell =[]
            self.all_buy =[]  
            
             
        elif now.minute % short_period ==0  and self.last_minute != now.minute:
            
            plt.close()
            plt.pause(pause_time)            
            
            self.last_minute = now.minute

            self.all_sell = self.all_sell + self.order_sell
            self.all_buy = self.all_buy + self.order_buy                 
           
            data_sell = create_Alltrades_frame(self.all_sell)
            data_sell['size']= data_sell['size'].values.astype(np.int64)
            data_buy = create_Alltrades_frame(self.all_buy)
            data_buy['size']= data_buy['size'].values.astype(np.int64)
            
    
                     
            if len(self.data_sell)>0 and len(self.data_buy)>0:

                 dif =  now - self.data_sell['timestamp'].iloc[0]
                 
                 if dif.seconds//60 < long_period:
                    data_sell =pd.concat([self.data_sell,data_sell],ignore_index=True, sort=False )  
                    data_buy = pd.concat([self.data_buy,data_buy], ignore_index=True, sort=False) 
                 else:
                    data_sell = pd.concat([self.data_sell[self.data_sell['timestamp']>(now - timedelta(minutes = long_period))],
                                         data_sell],ignore_index=True, sort=False)  
                    data_buy = pd.concat([self.data_buy[self.data_buy['timestamp']>(now - timedelta(minutes = long_period))],
                                         data_buy],ignore_index=True, sort=False)              
            
            
            big_buy = data_buy[(data_buy['size']>=15000)&(data_buy['size']<50000)]
            bigS_buy = data_buy[(data_buy['size']>=50000)&(data_buy['size']<150000)]
            bigG_buy = data_buy[data_buy['size']>=150000]
     
            big_sell = data_sell[(data_sell['size']>=15000 )&(data_sell['size']<50000)]
            bigS_sell = data_sell[(data_sell['size']>=50000)&(data_sell['size']<150000)]
            bigG_sell = data_sell[data_sell['size']>=150000]
            
       
            n = 10 # count of the top trade
            #trade from 15 000 to 50 000
            mostsell = big_sell['size'].value_counts().index.tolist()[:n]
            bigsell_show =  big_sell[big_sell['size'].isin(mostsell)]
            #
            mostsbuy = big_buy['size'].value_counts().index.tolist()[:n]
            bigbuy_show =  big_buy[big_buy['size'].isin(mostsbuy)]
            
            #trade from 50 000 to 150 000
            mostsellS = bigS_sell['size'].value_counts().index.tolist()[:n]
            bigsellS_show =  bigS_sell[bigS_sell['size'].isin(mostsellS)]
            #
            mostsbuyS = bigS_buy['size'].value_counts().index.tolist()[:n]
            bigbuyS_show =  bigS_buy[bigS_buy['size'].isin(mostsbuyS)]
            
            mostsellG = bigG_sell['size'].value_counts().index.tolist()[:3]
            bigsellG_show =  bigG_sell[bigG_sell['size'].isin(mostsellG)]
            #
            mostsbuyG = bigG_buy['size'].value_counts().index.tolist()[:3]
            bigbuyG_show =  bigG_buy[bigG_buy['size'].isin(mostsbuyG)]
            
            figure(figsize=(14,4), dpi=80)
            sns.scatterplot(x="timestamp", y="price",color = 'mediumseagreen', data=bigsell_show,s=10)#,marker="+"
            sns.scatterplot(x="timestamp", y="price",color = 'green',  data=bigsellS_show,s=10)
            sns.scatterplot(x="timestamp", y="price",  color='darkslategray' ,data=bigsellG_show,s=120) 
            
            sns.scatterplot(x="timestamp", y="price", color='coral', data=bigbuy_show,s=10)#marker="+",
            sns.scatterplot(x="timestamp", y="price",  color='indianred' ,data=bigbuyS_show,s=10)       
            sns.scatterplot(x="timestamp", y="price",  color='darkred' ,data=bigbuyG_show,s=120) 
            
            plt.draw()
            plt.pause(pause_time)
     
            self.order_buy =[]
            self.order_sell =[]
            


    def on_error(self,ws, error):
        print(error)

    def on_close(self):
        print ("Retry : %s" % time.ctime())
        time.sleep(10)
        start()

    def on_open(self,ws):
        print("### connected ###")

def start():
        websocket._logging._logger.level = -99
        
        liq = Liq()
        liq.ws.run_forever(dispatcher=rel)
        rel.signal(2, rel.abort)  # Keyboard Interrupt
        rel.dispatch()   
        



if __name__ == "__main__":

    start()
