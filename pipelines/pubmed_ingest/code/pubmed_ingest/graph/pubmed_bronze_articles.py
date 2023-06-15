from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *

def pubmed_bronze_articles(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("xml")\
        .option("rowTag", "MedlineCitation")\
        .option("ignoreNamespace", True)\
        .schema(
          StructType([
            StructField("Article", StructType([
              StructField("Abstract", StructType([
                StructField("AbstractText", ArrayType(
                StructType([
                  StructField("_Label", StringType(), True), StructField("_NlmCategory", StringType(), True), StructField("_VALUE", StringType(), True)
              ]), 
                True
          ), True)
              ]), True), StructField("ArticleTitle", StringType(), True), StructField("AuthorList", StructType([
                StructField("Author", ArrayType(
                StructType([
                  StructField("AffiliationInfo", StructType([
                    StructField("Affiliation", StringType(), True)
                  ]), True), StructField("CollectiveName", StringType(), True), StructField("ForeName", StringType(), True), StructField("Initials", StringType(), True), StructField("LastName", StringType(), True), StructField("Suffix", StringType(), True), StructField("_ValidYN", StringType(), True)
              ]), 
                True
          ), True), StructField("_CompleteYN", StringType(), True)
              ]), True), StructField("ELocationID", StructType([
                StructField("_EIdType", StringType(), True), StructField("_VALUE", StringType(), True), StructField("_ValidYN", StringType(), True)
              ]), True), StructField("GrantList", StructType([
                StructField("Grant", ArrayType(
                StructType([
                  StructField("Acronym", StringType(), True), StructField("Agency", StringType(), True), StructField("Country", StringType(), True), StructField("GrantID", StringType(), True)
              ]), 
                True
          ), True), StructField("_CompleteYN", StringType(), True)
              ]), True), StructField("Journal", StructType([
                StructField("ISOAbbreviation", StringType(), True), StructField("ISSN", StructType([
                  StructField("_IssnType", StringType(), True), StructField("_VALUE", StringType(), True)
                ]), True), StructField("JournalIssue", StructType([
                  StructField("Issue", StringType(), True), StructField("PubDate", StructType([
                    StructField("Day", LongType(), True), StructField("MedlineDate", StringType(), True), StructField("Month", StringType(), True), StructField("Season", StringType(), True), StructField("Year", LongType(), True)
                  ]), True), StructField("Volume", StringType(), True), StructField("_CitedMedium", StringType(), True)
                ]), True), StructField("Title", StringType(), True)
              ]), True), StructField("Language", ArrayType(StringType(), True), True), StructField("Pagination", StructType([
                StructField("MedlinePgn", StringType(), True)
              ]), True), StructField("PublicationTypeList", StructType([
                StructField("PublicationType", ArrayType(
                StructType([StructField("_UI", StringType(), True), StructField("_VALUE", StringType(), True)]), 
                True
          ), True)
              ]), True), StructField("VernacularTitle", StringType(), True), StructField("_PubModel", StringType(), True)
            ]), True), StructField("ChemicalList", StructType([
              StructField("Chemical", ArrayType(
              StructType([
                StructField("NameOfSubstance", StructType([
                  StructField("_UI", StringType(), True), StructField("_VALUE", StringType(), True)
                ]), True), StructField("RegistryNumber", StringType(), True)
            ]), 
              True
          ), True)
            ]), True), StructField("CitationSubset", StringType(), True), StructField("CommentsCorrectionsList", StructType([
              StructField("CommentsCorrections", ArrayType(
              StructType([
                StructField("Note", StringType(), True), StructField("PMID", StructType([
                  StructField("_VALUE", LongType(), True), StructField("_Version", LongType(), True)
                ]), True), StructField("RefSource", StringType(), True), StructField("_RefType", StringType(), True)
            ]), 
              True
          ), True)
            ]), True), StructField("DateCompleted", StructType([
              StructField("Day", LongType(), True), StructField("Month", LongType(), True), StructField("Year", LongType(), True)
            ]), True), StructField("DateRevised", StructType([
              StructField("Day", LongType(), True), StructField("Month", LongType(), True), StructField("Year", LongType(), True)
            ]), True), StructField("GeneralNote", ArrayType(
            StructType([StructField("_Owner", StringType(), True), StructField("_VALUE", StringType(), True)]), 
            True
          ), True), StructField("KeywordList", StructType([
              StructField("Keyword", ArrayType(
              StructType([StructField("_MajorTopicYN", StringType(), True), StructField("_VALUE", StringType(), True)]), 
              True
          ), True), StructField("_Owner", StringType(), True)
            ]), True), StructField("MedlineJournalInfo", StructType([
              StructField("Country", StringType(), True), StructField("ISSNLinking", StringType(), True), StructField("MedlineTA", StringType(), True), StructField("NlmUniqueID", StringType(), True)
            ]), True), StructField("MeshHeadingList", StructType([
              StructField("MeshHeading", ArrayType(
              StructType([
                StructField("DescriptorName", StructType([
                  StructField("_MajorTopicYN", StringType(), True), StructField("_Type", StringType(), True), StructField("_UI", StringType(), True), StructField("_VALUE", StringType(), True)
                ]), True), StructField("QualifierName", ArrayType(
                StructType([
                  StructField("_MajorTopicYN", StringType(), True), StructField("_UI", StringType(), True), StructField("_VALUE", StringType(), True)
              ]), 
                True
              ), True)
            ]), 
              True
          ), True)
            ]), True), StructField("NumberOfReferences", LongType(), True), StructField("OtherAbstract", StructType([
              StructField("AbstractText", StringType(), True), StructField("_Language", StringType(), True), StructField("_Type", StringType(), True)
            ]), True), StructField("OtherID", ArrayType(
            StructType([StructField("_Source", StringType(), True), StructField("_VALUE", StringType(), True)]), 
            True
          ), True), StructField("PMID", StructType([
              StructField("_VALUE", LongType(), True), StructField("_Version", LongType(), True)
            ]), True), StructField("PersonalNameSubjectList", StructType([
              StructField("PersonalNameSubject", ArrayType(
              StructType([
                StructField("ForeName", StringType(), True), StructField("Initials", StringType(), True), StructField("LastName", StringType(), True)
            ]), 
              True
          ), True)
            ]), True), StructField("SpaceFlightMission", ArrayType(StringType(), True), True), StructField("_Owner", StringType(), True), StructField("_Status", StringType(), True)
        ])
        )\
        .load("dbfs:/prophecy-samples/med-advisor/pubmed/")
