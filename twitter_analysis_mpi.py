import json
import re
import itertools
import time
from mpi4py import MPI
from operator import itemgetter 

twitterpath = "./bigTwitter.json"

#json version lang & hashtag
def twitterDataProcess(filepath, comm):
    twitterhashtagDic = {} #count hashtag
    twitterLangDic = {} #count lang
    # print("core",comm.rank,"start")
    corestarttime = time.time()
    with open(filepath,'r', encoding='utf-8') as f:
        for idx, line in enumerate(f):
            if (idx % comm.size) == comm.rank:
                line = line.strip()[:-1]
                try:
                    data_json = json.loads(line)

                    #hashtag
                    for i in data_json["doc"]["entities"]["hashtags"]:
                        hashtag = re.findall('[^!"#$%&\\\(\)*+,-./:;<=>?@\[\]^`{|}~\s]*', i["text"].lower())[0]
                        if hashtag in twitterhashtagDic.keys():
                            twitterhashtagDic[hashtag] += 1 #if already had key in Dic, then ++
                        else:
                            twitterhashtagDic[hashtag] = 1 #if not, count 1
                    
                    #lang
                    lang = data_json["doc"]["metadata"]["iso_language_code"]
                    if lang in twitterLangDic.keys():
                        twitterLangDic[lang] += 1 #if already had key in Dic, then ++
                    else:
                        twitterLangDic[lang] = 1 #if not, count 1

                except: #if can't read, just continue
                    continue
        coreendtime = time.time()
        print("rank[{0}] processing time: {1} s".format(comm.rank, coreendtime-corestarttime))
        return twitterhashtagDic, twitterLangDic

def getTidyDicfromGather(mylist):
    tidy_dictionary = {}
    for i in mylist:
        for key in i:
            if key in tidy_dictionary.keys():
                tidy_dictionary[key] = tidy_dictionary[key] + i[key]
            else:
                tidy_dictionary[key] = i[key]
    return tidy_dictionary

def getTopN(myDic, topN):
    resultDic = dict(sorted(myDic.items(), key = itemgetter(1), reverse = True)[:topN])
    return resultDic

def getTopNfromGather(mylist, topN):
    myDic = getTidyDicfromGather(mylist)
    return getTopN(myDic, topN)

def langCodeAddName(myDic):
    iso_639_choices = [('ab', 'Abkhaz'),('aa', 'Afar'),('af', 'Afrikaans'),('ak', 'Akan'),('sq', 'Albanian'),('am', 'Amharic'),('ar', 'Arabic'),('an', 'Aragonese'),('hy', 'Armenian'),('as', 'Assamese'),('av', 'Avaric'),('ae', 'Avestan'),('ay', 'Aymara'),('az', 'Azerbaijani'),('bm', 'Bambara'),('ba', 'Bashkir'),('eu', 'Basque'),('be', 'Belarusian'),('bn', 'Bengali'),('bh', 'Bihari'),('bi', 'Bislama'),('bs', 'Bosnian'),('br', 'Breton'),('bg', 'Bulgarian'),('my', 'Burmese'),('ca', 'Catalan; Valencian'),('ch', 'Chamorro'),('ce', 'Chechen'),('ny', 'Chichewa; Chewa; Nyanja'),('zh', 'Chinese'),('cv', 'Chuvash'),('kw', 'Cornish'),('co', 'Corsican'),('cr', 'Cree'),('hr', 'Croatian'),('cs', 'Czech'),('da', 'Danish'),('dv', 'Divehi; Maldivian;'),('nl', 'Dutch'),('dz', 'Dzongkha'),('en', 'English'),('eo', 'Esperanto'),('et', 'Estonian'),('ee', 'Ewe'),('fo', 'Faroese'),('fj', 'Fijian'),('fi', 'Finnish'),('fr', 'French'),('ff', 'Fula'),('gl', 'Galician'),('ka', 'Georgian'),('de', 'German'),('el', 'Greek, Modern'),('gn', 'Guaraní'),('gu', 'Gujarati'),('ht', 'Haitian'),('ha', 'Hausa'),('he', 'Hebrew (modern)'),('hz', 'Herero'),('hi', 'Hindi'),('ho', 'Hiri Motu'),('hu', 'Hungarian'),('ia', 'Interlingua'),('id', 'Indonesian'),('ie', 'Interlingue'),('ga', 'Irish'),('ig', 'Igbo'),('ik', 'Inupiaq'),('io', 'Ido'),('is', 'Icelandic'),('it', 'Italian'),('iu', 'Inuktitut'),('ja', 'Japanese'),('jv', 'Javanese'),('kl', 'Kalaallisut'),('kn', 'Kannada'),('kr', 'Kanuri'),('ks', 'Kashmiri'),('kk', 'Kazakh'),('km', 'Khmer'),('ki', 'Kikuyu, Gikuyu'),('rw', 'Kinyarwanda'),('ky', 'Kirghiz, Kyrgyz'),('kv', 'Komi'),('kg', 'Kongo'),('ko', 'Korean'),('ku', 'Kurdish'),('kj', 'Kwanyama, Kuanyama'),('la', 'Latin'),('lb', 'Luxembourgish'),('lg', 'Luganda'),('li', 'Limburgish'),('ln', 'Lingala'),('lo', 'Lao'),('lt', 'Lithuanian'),('lu', 'Luba-Katanga'),('lv', 'Latvian'),('gv', 'Manx'),('mk', 'Macedonian'),('mg', 'Malagasy'),('ms', 'Malay'),('ml', 'Malayalam'),('mt', 'Maltese'),('mi', 'Māori'),('mr', 'Marathi (Marāṭhī)'),('mh', 'Marshallese'),('mn', 'Mongolian'),('na', 'Nauru'),('nv', 'Navajo, Navaho'),('nb', 'Norwegian Bokmål'),('nd', 'North Ndebele'),('ne', 'Nepali'),('ng', 'Ndonga'),('nn', 'Norwegian Nynorsk'),('no', 'Norwegian'),('ii', 'Nuosu'),('nr', 'South Ndebele'),('oc', 'Occitan'),('oj', 'Ojibwe, Ojibwa'),('cu', 'Old Church Slavonic'),('om', 'Oromo'),('or', 'Oriya'),('os', 'Ossetian, Ossetic'),('pa', 'Panjabi, Punjabi'),('pi', 'Pāli'),('fa', 'Persian'),('pl', 'Polish'),('ps', 'Pashto, Pushto'),('pt', 'Portuguese'),('qu', 'Quechua'),('rm', 'Romansh'),('rn', 'Kirundi'),('ro', 'Romanian, Moldavan'),('ru', 'Russian'),('sa', 'Sanskrit (Saṁskṛta)'),('sc', 'Sardinian'),('sd', 'Sindhi'),('se', 'Northern Sami'),('sm', 'Samoan'),('sg', 'Sango'),('sr', 'Serbian'),('gd', 'Scottish Gaelic'),('sn', 'Shona'),('si', 'Sinhala, Sinhalese'),('sk', 'Slovak'),('sl', 'Slovene'),('so', 'Somali'),('st', 'Southern Sotho'),('es', 'Spanish; Castilian'),('su', 'Sundanese'),('sw', 'Swahili'),('ss', 'Swati'),('sv', 'Swedish'),('ta', 'Tamil'),('te', 'Telugu'),('tg', 'Tajik'),('th', 'Thai'),('ti', 'Tigrinya'),('bo', 'Tibetan'),('tk', 'Turkmen'),('tl', 'Tagalog'),('tn', 'Tswana'),('to', 'Tonga'),('tr', 'Turkish'),('ts', 'Tsonga'),('tt', 'Tatar'),('tw', 'Twi'),('ty', 'Tahitian'),('ug', 'Uighur, Uyghur'),('uk', 'Ukrainian'),('ur', 'Urdu'),('uz', 'Uzbek'),('ve', 'Venda'),('vi', 'Vietnamese'),('vo', 'Volapük'),('wa', 'Walloon'),('cy', 'Welsh'),('wo', 'Wolof'),('xh', 'Xhosa'),('yi', 'Yiddish'),('yo', 'Yoruba'),('za', 'Zhuang, Chuang'),('zu', 'Zulu')]
    for i in iso_639_choices:
        for key in myDic:
            if i[0] == key:
                new_name = '{0} ({1})'.format(i[1], key)
                myDic[new_name] = myDic.pop(key)
    resultDic = dict(sorted(myDic.items(), key = itemgetter(1), reverse = True))
    return resultDic

def main():
    start_time = time.time()
    comm = MPI.COMM_WORLD
    hashtag_count, lang_count = twitterDataProcess(twitterpath, comm)
    hashtag_gather = comm.gather(hashtag_count)
    lang_gather = comm.gather(lang_count)
    if comm.rank == 0:
        lang_result = langCodeAddName(getTopNfromGather(lang_gather, 10))
        hashtag_result = getTopNfromGather(hashtag_gather, 10)
        end_time = time.time()
        process_time = end_time - start_time
        print("Total processing time for is {0} seconds ({1} core(s))".format(process_time, comm.size))
        # print(lang_result)
        # print(hashtag_result)
        print("Top 10 Languages:")
        for idx, item in enumerate(lang_result):
            print("{0}. {1} : {2}".format(idx+1, item, lang_result[item]))
        print("Top 10 Hashtags:")
        for idx, item in enumerate(hashtag_result):
            print("{0}. {1} : {2}".format(idx+1, item, hashtag_result[item]))

if __name__ == "__main__":
    main()