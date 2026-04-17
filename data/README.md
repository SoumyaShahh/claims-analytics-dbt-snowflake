\# Raw Data



This folder is \*\*excluded from version control\*\* (`.gitignore`) because the source CSVs are too large for GitHub (>100 MB total) and aren't owned by this project.



\## What goes here



This project uses \[Synthea™](https://synthea.mitre.org/) — open-source synthetic patient data published by \[MITRE Corporation](https://www.mitre.org/). The data is HIPAA-free, license-free, and designed specifically for healthcare analytics development and testing.



\## How to reproduce the dataset



1\. Download the \*\*1,000 sample synthetic patient records (CSV format)\*\* from:

&#x20;  \*\*https://synthea.mitre.org/downloads\*\*



2\. Unzip the archive into this `data/` folder.



3\. Verify the following CSV files are present (these are the ones used by this project):

&#x20;  - `patients.csv`

&#x20;  - `encounters.csv`

&#x20;  - `claims.csv`

&#x20;  - `conditions.csv`

&#x20;  - `providers.csv`

&#x20;  - `organizations.csv`

&#x20;  - `payers.csv`

&#x20;  - `payer\_transitions.csv`



4\. Load the data into Snowflake `CLAIMS\_DB.RAW` schema (see `/snowflake/setup.sql` and project README for full instructions).



\## Approximate row counts after load



| Table | Rows |

|---|---:|

| RAW\_PATIENTS | \~1,200 |

| RAW\_ENCOUNTERS | \~61,000 |

| RAW\_CLAIMS | \~118,000 |

| RAW\_CONDITIONS | \~38,000 |

| RAW\_PROVIDERS | \~5,000 |

| RAW\_ORGANIZATIONS | \~1,100 |

| RAW\_PAYERS | 10 |

| RAW\_PAYER\_TRANSITIONS | \~53,000 |



\## Notes



\- Synthea CSV schemas can vary slightly between versions. If column-mismatch errors occur during load, the table DDL in `/snowflake/setup.sql` may need minor adjustment.

\- Files like `claims\_transactions.csv` (\~300 MB), `observations.csv`, and `imaging\_studies.csv` are not used by this project's models and can be safely omitted.



\## Citation



> Walonoski J, Kramer M, Nichols J, et al. \*Synthea: An approach, method, and software mechanism for generating synthetic patients and the synthetic electronic health care record.\* Journal of the American Medical Informatics Association. 2018;25(3):230-238. https://doi.org/10.1093/jamia/ocx079

