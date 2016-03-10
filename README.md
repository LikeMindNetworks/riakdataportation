# riakdataportation

## Build/Install

* Install golang on your machine, making sure you have ```go``` command on PATH
    * [https://golang.org/dl/](https://golang.org/dl/)
* Run ```go get github.com/likemindnetworks/riakdataportation/portation```
* That should be it

## Usage
Note that flags/options (e.g. -a) must go before arguments (e.g. import)
```
Riak Data Portation Tool
Usage: portation <export|import> <riak host with port>
  -a string
    	app name
  -c int
    	number of connections (default 20)
  -d string
    	data version
  -i string
    	input file for import
  -o string
    	output folder for export result (default ".")
  -v	print version

```

### To Export data out of the DB

* **-a** Required. name of the application, e.g. teamDental
* **-o** the folder where the output is written to, default to current direction
* set first argument after the flags/options to **export**
* set second argument after the flags/options to **url to the riak protocol buffer port**

Example:
```
portation \
  -a teamCS \
  -o . \
  export Riak-dev-ELB-749646943.us-east-1.elb.amazonaws.com:8087
```

This will write to a file named similar to ```teamCS.2016-03-10T16:41:56-05:00.bin```. The format is:
```
<app name>@<data version>.<timestamp>.bin
```

### To Import data into the DB

* **-i** the path to data file generated from exporting
* **-a** name of the application, e.g. teamDental, _**When importing, the app name can override to app name in the data file, this allows data from one app to be imported into another app. This requires caution and a confirmation will be asked**_
* set first argument after the flags/options to **import**
* set second argument after the flags/options to **url to the riak protocol buffer port**

Example:
```
portation \
  -a teamCS \
  -i ./teamCS.2016-03-10T16:41:56-05:00.bin \
  import Riak-dev-ELB-749646943.us-east-1.elb.amazonaws.com:8087
```

### Data File Format
A request followed by its corresponding response. There are only two types request and response pairs:

* RpbGetReq, RpbGetResp
* DtFetchReq, DtFetchResp

```
|protocol header (5 bytes)|request protocol message|protocol header (5 bytes)|response protocol message|...
```

For detailed specs: [http://docs.basho.com/riak/latest/dev/references/protocol-buffers/](http://docs.basho.com/riak/latest/dev/references/protocol-buffers/)
