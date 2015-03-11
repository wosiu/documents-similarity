%default input '../input2'
%default shingle_size 3
%default hash_functions_number 5


register 'shingling/target/shingling-1.0-SNAPSHOT.jar'
register 'minhashing/target/minhashing-1.0-SNAPSHOT.jar'
register 'lsh/target/lsh-1.0-SNAPSHOT.jar'

define dataBagStringConcate DataBagStringConcate();
define shingle Shingler('$shingle_size');
define minhashing Minhashing('$hash_functions_number');

set pig.splitCombination false;

-------------------------------------------------------------------------------------------------
-- Prepare data
-------------------------------------------------------------------------------------------------
A = LOAD '$input' USING PigStorage('\t', '-tagFile') AS (docname:chararray, doctext:chararray);
-- we didn't serialize documents, so now each paragraph is separated to row
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
-- using dummy value such as 1 we use JOIN as well performed CROSS
-- we use 'replicated' to keep SHINGLE_SUM_NUM in memory (there is one row with total number of shingle)
K = JOIN J by 1, SHINGLE_SUM_NUM by 1 USING 'replicated'; 
L = FOREACH K GENERATE docname as docname, minhashing(shingle_ids, shingle_total_num) as signature;
DUMP L;

-------------------------------------------------------------------------------------------------
-- LSH
-------------------------------------------------------------------------------------------------



