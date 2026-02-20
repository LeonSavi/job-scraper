from concurrent.futures import ThreadPoolExecutor
import queue
from pathlib import Path
import requests

# https://www.youtube.com/watch?v=FbtCl9jJyyc


class ProxiesChecker():

    def __init__(self, root:str = 'proxies',save_untested:bool= False):

        Path(root).mkdir(parents=True, exist_ok=True)

        self.ROOT = root

        self.proxy_link = 'https://raw.githubusercontent.com/proxifly/free-proxy-list/refs/heads/main/proxies/all/data.txt'

        self._loadlist()

        if save_untested and self.untested_proxies:
            with open(f"{self.ROOT}/untested_proxies.txt",'w') as w:
                w.write('\n'.join(self.untested_proxies))
          

    def _loadlist(self):
        
        resp = requests.get(self.proxy_link).content

        untested_proxies = []

        for i in str(resp).split('\\n'):
            prox = i.partition('://')[2]
            untested_proxies.append(prox)
        
        self.untested_proxies = untested_proxies


    def get_valid_proxies(self):

        if not Path(self.ROOT+'/valid_proxies.txt').exists():
            print('--- NO VALID PROXIES FILE FOUND: running script ---')
            self.run()
        else:
            with open(f'{self.ROOT}/valid_proxies.txt','r') as r:
                self.valid_proxies = r.read().split('\n')

        return self.valid_proxies



    def _check_proxies(self,proxy):

        try:
            res = requests.get(
                'https://ipinfo.io/json',
                proxies={
                    'https':proxy
                },
                timeout=10)

            if res.status_code == 200:
                print(f'Valid Proxy: {proxy}')
                return proxy
            
        except:
            return None
    
    def run(self):

        print(f'\n---- CHECKING {len(self.untested_proxies)} proxies')

        with ThreadPoolExecutor(max_workers=20) as exec:

            results = exec.map(self._check_proxies,self.untested_proxies)    
            
        self.valid_proxies = [i for i in results if not i is None]

        with open(f'{self.ROOT}/valid_proxies.txt','w') as w:
            w.write('\n'.join(self.valid_proxies))

    
        