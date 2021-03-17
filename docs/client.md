# Client Documentation


## Summary
The clients will connect to the work server and receive work from the work server. 

## Description 
Multiple clients will be able to connect to the work server at once. A client will also try to get a `client_id` if it doesn't have one already. Client will request work from the work server and the client will return the work when it is complete. If the work server can't provide the client with work the client will then sleep for a minute and then try to request work again. It will follow that cycle for as long it is connected to the work server. 

