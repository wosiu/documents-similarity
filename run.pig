-- Michał Woś
-- Big data processing course, 2015
-- University of Warsaw

%default input '../input2'
%default shingle_size 3
-- hash_functions_number == signature size
%default hash_functions_number 8
-- band_size should divide hash_functions_number 
%default band_size 2

register 'shingling/target/shingling-1.0-SNAPSHOT.jar'
register 'minhashing/target/minhashing-1.0-SNAPSHOT.jar'
register 'lsh/target/lsh-1.0-SNAPSHOT-jar-with-dependencies.jar'

define dataBagStringConcate DataBagStringConcate();
define shingle Shingler('$shingle_size');
define minhashing Minhashing('$hash_functions_number');
define createBands BandCreator('$band_size');
define createPairs PairsCreator();

set pig.splitCombination false;

-------------------------------------------------------------------------------------------------
-- Prepare data
-------------------------------------------------------------------------------------------------
A = LOAD '$input' USING PigStorage('\t', '-tagFile') AS (docname:chararray, doctext:chararray);
-- we didn't serialize documents (one file = one doc), so now paragraphs are separated by EOL to rows
-- we want to concatenate paragraphs from same documents
B = GROUP A BY docname;
C = FOREACH B GENERATE group AS docname, FLATTEN(dataBagStringConcate(A.doctext)) AS doctext;


-------------------------------------------------------------------------------------------------
-- Shingle
-------------------------------------------------------------------------------------------------
D = FOREACH C GENERATE docname, shingle(doctext) AS docshingle;
E = FOREACH D GENERATE docname, FLATTEN(docshingle) as shingle; 

-- Prepare sum of shingle
F = FOREACH E GENERATE shingle;
SHINGLE_SUM = DISTINCT F;

SHINGLE_SUM_NUM = FOREACH (GROUP SHINGLE_SUM ALL) {
        GENERATE COUNT(SHINGLE_SUM) as shingle_total_num;
    };

-- sort and add ID to each shingle
SHINGLE_SUM = RANK SHINGLE_SUM by shingle ASC;
SHINGLE_SUM = FOREACH SHINGLE_SUM GENERATE rank_SHINGLE_SUM as shingle_id, shingle;

-- join shingle id with shingle for each document
G = JOIN E by shingle, SHINGLE_SUM by shingle;
H = FOREACH G GENERATE docname, shingle_id;
I = GROUP H by docname;
J = FOREACH I generate group as docname, H.shingle_id as shingle_ids;


-------------------------------------------------------------------------------------------------
-- MinHashing
-------------------------------------------------------------------------------------------------
-- add info about total shingles number to each row 
-- by using dummy value such as 1 we use JOIN as well performed CROSS
-- we use 'replicated' to keep SHINGLE_SUM_NUM (which has 1 row) in memory to preserve nice performance
K = JOIN J by 1, SHINGLE_SUM_NUM by 1 USING 'replicated'; 
L = FOREACH K GENERATE docname as docname, minhashing(shingle_ids, shingle_total_num) as doc_signature;

-------------------------------------------------------------------------------------------------
-- LSH
-------------------------------------------------------------------------------------------------
M = FOREACH L GENERATE docname as docname, FLATTEN(createBands(doc_signature)) as (band_signature, band_level);
O = GROUP M by (band_level, band_signature);
BUCKETS = FOREACH O GENERATE M.docname as bucket;
BUCKETS = DISTINCT BUCKETS;
BUCKETS = FILTER BUCKETS by (SIZE(bucket) > 1);
DOC_PAIRS = FOREACH BUCKETS GENERATE createPairs(bucket);
DUMP DOC_PAIRS;

