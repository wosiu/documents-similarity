# documents-similarity
Big data processing course - housework

POLECENIE: Znajdywanie podobnych zbiorów (o hashowaniu)
1. napisz w Javie program znajdujący podobne dokumenty przy pomocy Shingling, Minhashing i Local-Sensitive Hashing
2. program ma składać się z trzech podprogramów, które przekazują dane między sobą przez pliki; zakładamy że macierze są rzadkie i tak je reprezentujemy


QUICK SET-UP without map-reduce (java code only):
1. budujemy projekt: mvn clean install

2. 
a) odpalenie shingle:
java -cp shingling/target/shingling-1.0-SNAPSHOT-jar-with-dependencies.jar NoMapReduceResolver dirPath shingleSize
np:
java -cp shingling/target/shingling-1.0-SNAPSHOT-jar-with-dependencies.jar NoMapReduceResolver sample_input/ 3

Wynik zapisał się w pliku 'shingle-output.json' w katalogu projektu.
Jest to mapowanie dokumentów na listę id'ków ich shingli.

b) odpalenie minhashing:

java -cp minhashing/target/minhashing-1.0-SNAPSHOT-jar-with-dependencies.jar hashFunctionNumber
gdzie `hashFunctionNumber` == dlgosc sygnatury per dokument
np:
java -cp minhashing/target/minhashing-1.0-SNAPSHOT-jar-with-dependencies.jar 100

Wynik zapisał się w pliku 'minhash-output.json' w katalogu projektu.
Jest to mapowanie dokumentów na ich sygnatury. 

c) odpalenie LSH:

java -cp lsh/target/lsh-1.0-SNAPSHOT-jar-with-dependencies.jar bandSize
gdzie `bandSize` powinno dzielic `hashFunctionNumber`
np:
java -cp lsh/target/lsh-1.0-SNAPSHOT-jar-with-dependencies.jar 5

Wynik został wypisany na stdout.
Są to pary dokumentów, które wg LSH powinny zostać sprawdzone (co najmniej po jednym bandzie wpadły do tego samego bucketa).


QUICK SET-UP FOR PIG (poza zadaniem zaliczeniowym):
1. budujemy projekt: mvn clean install

2. w skrypcie run.pig ustawiamy ścieżkę do katalogu z dokumentami - Parametr 'input'. 
Każdy dokument powinien się znajdować w osobnym pliku. 

3. Wymagany ściągnięty pig, np.:
http://ftp.piotrkosoft.net/pub/mirrors/ftp.apache.org/pig/latest/

4. Odpalenie lokalnie (bez hdfs):
sciezka_do_wypakowanego_piga/bin/pig -x local run.pig > results

Wynik jest DUMPowany. Wystarczy przekierować stdout do pliku. W powyższym przykładzie wynik znajdzie sie w pliku 'results'.
Są to pary dokumentów, które wg LSH powinny zostać sprawdzone (co najmniej po jednym bandzie wpadły do tego samego bucketa).

5. Zmiana ustawień domyslnych parametrow UDFów (shingle, minhashing, lsh) w skrypcie run.pig na samej górze

6. Aby wypisac produkty uboczne po kolejnych etapach (shingle, minhashing, lsh) wystarczy odkomentowac DUMP'y w skrypcie PIGowym we wskazanych miejsach.
