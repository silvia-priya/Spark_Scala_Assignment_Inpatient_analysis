# Spark_Scala_Assignment_Inpatient_analysis
Scala program helps you to understand the process of reading the Inpatient file and perform certain use cases using the spark RDD operations.

Steps to do:-
=============

The schema of the input file is as follows.

DRGDefinition -- Diagnosis Related Group
ProviderId -- Unique provider id 
ProviderName -- Unique name of the provider
ProviderStreetAddress -- Address of the provider 
ProviderCity -- City of the provider
ProviderState -- State of the provider 
ProviderZipCode -- Zip code of the provider 
HospitalReferralRegionDescription -- Region description where the hospital is located
TotalDischarges -- Total number of discharges happened in that region
AverageCoveredCharges -- Average charges covered by the hospital 
AverageTotalPayments -- Average total payments covered overall
AverageMedicarePayments -- Average medical payements.

Use cases to do:-
==================

1. Find out the city and the state name which has the highest and lowest number of discharge cases.     
2. What are all the different referral regions under every state with a code AL and CA which has a discharge rate of more than 20.

Tips to take care:-
===================

1. Build one scala code and enclose all the RDD's needed in it.
2. Try to adopt the trim() function whenever you are reading a string field.
3. Load the finally obtained result into a file using saveAsTextFile method.
