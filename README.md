# Scanner Minute - a minute resolution stock scanner


## Some Calculations for Minute timeframe
From 09:00 to 01:00 the next day is 16 hours = 960 minutes.

The limit for a single download in 50000

50000 / 960 = 

So we can download approximately two months 


## Uncooked
I want to add file imaging_utils.py to ScannerMinute/src. In this file, there will be the following functions:
1) A python function that check whether node.js is installed. If not, ask whether to install. If the answer is yes, install node.js in node_server/ under the root directory.
2) A python function that removes the installation of node.js
3) A function that runs the server 

Under tst/tst_node.js create a script that operates cheking whether node.js is installed and install it if not. Then, run the server with some default parameters that shows a static webpage with node so we know it works.