# MY SERVICE BUS PERSISTENCE

## Run  

Enusure that environment variable "**HOME**" exists.
It should point to location with **.myservicebus-persistence** file!

**.myservicebus-persistence** content:
```
QueuesConnectionString: <Connection string to azure storage account>
MessagesConnectionString: <Connection string to azure storage account>
LoadBlobPagesSize: 8192
FlushQueuesSnapshotFreq: 00:00:01
FlushMessagesFreq: 00:00:01
MaxResponseRecordsAmount: 500
DeleteTopicSecretKey: SecretKeyString
```
Install rust: https://www.rust-lang.org/tools/install
execute: **cargo run --release**


## Changes

### 2.2.4
* Timeouts on PageBlob operations are 5 sec
* Create container if not exists now is retried
* Now we can read range of messages as well
