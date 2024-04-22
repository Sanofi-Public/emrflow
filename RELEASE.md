# Release History

## 0.1.0
Run PySpark jobs in EMR for Workbench
* User-friendly CLI to assist in configuring, submitting, tracking EMR jobs
* Support EMR Serverless


## 0.1.1
Adds Exception handling in when no-logs are available to display

## 0.1.2
Add APP_GEN1 tag in every run to assist in tracking for OneAI platform team


## 0.1.3
* Alpha release of EMR EKS support
* Logs are now printed periodically


## 0.1.4
* Add option for ping_duration when tracking the job status
* Add option to add tags for the jobs
* Add option to add execution timeout for a job
* During code package have the ability to add location path


## 1.0.0
* Removed EMR EKS support (temporarily)
* Refactored code
* Add more functionality to build dependencies and run
* Ability to only show appended logs

## 1.1.0
* Rename arguments of package dependencies
* functions will have a return response (useful when using library as an API)
* Fix minor bugs

## 1.2.0
* fix minor bugs

## 1.2.1
* add isort and pre-commit in dev
* remove unused imports

## 1.3.0
* add more configurations to build package
* remove dependency of implicit tags
* refactor code
* udate docstrings
* update compiling dependency package to remove hidden folders
* added test cases
* fix minor bugs
* updated version of aiohttp > 3.9.2

## 1.4.0
* Added doc-strings
* updated build and run command configurations
* refactored redundant code and library requirements

## 1.4.1
* update requirements
