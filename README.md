# Data-Validation-Guard

## 1. Introduction 

This repository aims to validate data retrieved and processed mainly from Maganamed & MovisensXS as part of systems from IMMERSE to cover a set of requirements from the Data Management Plan using Data Validation Plan (DVP) version 7, which describes a set of rules or expected outcomes to control data quality.
The project's architecture is composed as follows:

- Validation rules: Tailored functions according to the rules from DVP version 7.
- Cleaning process: Set of methods which apply changes when issues pop up during the validation process. Once all the changes are applied,
a new database is created with the updated changes.

Prior to validating the set of rules, a validation for IDs only is carried out to control issues like: 
- duplications in a simple and robust way
- typos 
- undefined and/or missing ids using "Master ID" files.

## 2. Requirements 
- Connection to the Research Database with all system sources of IMMERSE.
- Master ID files with the final IDs accepted for Maganamed, MovisensXS,DMMH, and REDCap.
- Defined rulebooks for each system which dictate the changes to apply to the tricky IDS. 
The script generates a rulebook per system in IMMERSE, however manual changes are required to do! To apply these changes, 
check the Master ID files. 

The following script carries several tasks after data has been processed in previous scripts IMMERSE0 to IMMERSE1 from this Projects' repository: 

- immerse0_checking_each_id_list
- immerse1_preprocessing_dmmh
- immerse1_preprocessing_maganamed
- immerse1_preprocessing_movisensESM
- immerse1_preprocessing_movisens_sensing
- immerse1_preprocessing_redcap_idLists
- immerse1_preprocessing_redcap_idLists
- sensing_location_metadata_anonymize
