# Redis Database Documentation

## Database Format

The database has three hashes, with the names:

`jobs_waiting`, `jobs_in_progress`, and `jobs_done`.

Each of these hashes will store jobs for clients to process.

Keys in the hashes will be integers, the job ids of the jobs.
These keys will be mapped to integers, the values to be processed.

Additionally, the database has an integer value storing the number of clients:
`total_num_client_ids`.