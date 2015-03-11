# documents-similarity
Big data processing course - housework

POLECENIE: Znajdywanie podobnych zbiorów (o hashowaniu)
1. napisz w Javie program znajdujący podobne dokumenty przy pomocy Shingling, Minhashing i Local-Sensitive Hashing
2. program ma składać się z trzech podprogramów, które przekazują dane między sobą przez pliki; zakładamy że macierze są rzadkie i tak je reprezentujemy

QUICK SET-UP:
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
