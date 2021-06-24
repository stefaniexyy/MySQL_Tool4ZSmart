#!//usr/local/bin/python3.9

import importlib
import sys
import time
import datetime
import os
import re
from io import StringIO

importlib.reload(sys)
time1 = time.time()
 
import os.path
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator,XMLConverter, HTMLConverter, TextConverter
from pdfminer.layout import LTTextBoxHorizontal,LAParams

def pdf2dat(input_file,output_file):
    print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'] Process file:'+input_file+'\n')
    if os.path.exists(output_file):
        os.remove(output_file) 
    with open(input_file,'rb') as pdf_file:
        parser=PDFParser(pdf_file)
        document = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        laparams = LAParams()
        device = PDFPageAggregator(rsrcmgr,laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr,device)
        for page in PDFPage.create_pages(document):
            interpreter.process_page(page)
            layout = device.get_result()
            for x in layout:
                if(isinstance(x,LTTextBoxHorizontal)):
                    with open(output_file,'a') as dat_file:
                        results = x.get_text().encode('utf-8')
                        dat_file.write(str(results,encoding = "utf8").strip())
    return 1

def pdf2dat2(input_file,output_file):
    print('['+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'] Process file:'+input_file)
    if os.path.exists(output_file):
        os.remove(output_file)
    file_out=open(output_file,'a')
    with open(input_file,'rb') as pdf_file:
        parser=PDFParser(pdf_file)
        document = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        laparams = LAParams()
        retstr = StringIO()
        device = TextConverter(rsrcmgr,retstr,laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr,device)
        #[0,1,2,3] means the 1 2 3 4 page
        for page in PDFPage.get_pages(pdf_file,):
            interpreter.process_page(page)
            text = retstr.getvalue()
            text=text.strip()
            file_out.write('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n')
            file_out.write(text+'\n')
            file_out.write('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n')
    return 1

#pdf2dat2('/soft/rec/rec/data/input/pdf/3475_961_VOIP.pdf','/soft/rec/rec/data/tmp/3475_961_VOIP.dat')
print('##############################################################################################')
print('#Require paremente "SRC" or "TAG"')
print('##############################################################################################')

curr_dir=os.path.split(os.path.realpath(__file__))[0]
in_path='/soft/rec/xyy/data/pdf/'
output_file='/soft/rec/xyy/data/dat/'
#if sys.argv[1]=='SRC':
#    in_path=curr_dir+'/../data/input/src_pdf/'
#    if not os.path.exists(curr_dir+'/../data/tmp/src_dat'):
#        os.makedirs(curr_dir+'/../data/tmp/src_dat')
#    output_file=curr_dir+'/../data/tmp/src_dat'
#elif sys.argv[1]=='TAG':
#    in_path=curr_dir+'/../data/input/tag_pdf/'
#    if not os.path.exists(curr_dir+'/../data/tmp/tag_dat'):
#        os.makedirs(curr_dir+'/../data/tmp/tag_dat')
#    output_file=curr_dir+'/../data/tmp/tag_dat'

print(in_path)
for i in os.listdir(in_path):
    if re.search(r'\.pdf$',i):
        infile=in_path+i
        outfile=output_file+'/'+re.search(r'(.+)\.pdf$',i).group(1)+'.dat'
        pdf2dat2(infile,outfile)
        