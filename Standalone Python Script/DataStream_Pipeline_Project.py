''' JavaScript Object Notation '''
import json
import re
import os
from jsonschema import validate
import pandas as pd
import PySimpleGUI as sg
import requests

#Global variable
df= pd.DataFrame()
RestAPI_documents = {}
    
def fix_backslash_issue(filename):
 with open(filename, 'r') as file :
  filedata = file.read().replace("\\", '')
 with open(filename, 'w') as file:
  file.write(filedata)
        
def URL_Validator(URL_input):
 regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
 return (re.match(regex, URL_input) is not None) 
    
def reload_function(): 
 __name__()

def category_keys_check(json_categories):    
 json_schema  = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "classification": {"type": "string"},
        "lastModified": {"type": "string"},
        "created": {"type": "string"},
        "creatorId": {"type": "string"},
        "path": {"type": "string"},
    },
}
 length=len(json_categories['documents'])
 for key in range(1, length):    
    try:
        validate(instance=json_categories['documents'][key], schema=json_schema)
    except:
        return False
 return True

def json_normalize_panda(categories_dict): 
     df = pd.json_normalize(categories_dict)
     df = df[['classification','id','path']]
     df.columns = df.columns.str.rstrip()
     return df

#TASK1.A
def get_json_from_api(doc_json, catg_json):   
 try:
     response1 = requests.get(doc_json)
     categories_dict = response1.json()
     if category_keys_check(categories_dict)==True:
      df=json_normalize_panda(categories_dict['documents'])
     else:
       sg.popup("Message:","=======","Expection: The structure of Categroy JSON file's keys is not valid. Please recheck!") 
       reload_function
 except ValueError as err:
      sg.popup("Message:","=======",'Loading Error'+err) 
      reload_function

 try:
         response2 = requests.get(catg_json)
         RestAPI_documents = response2.json()
         sg.popup("Message:","=======","Both JSON objects [Document] and [Category] are successfully received.")
 except ValueError as err:
     sg.popup("Message:","=======",'Loading Error'+err)
     reload_function
 return (df, RestAPI_documents)
     
#TASK1.B
def merge_path_with_document(doc_dict, catg_df):     
 key=0
 y = pd.DataFrame()
 catg_df['counter']=0
 if 'documents' in doc_dict:
    length=len(doc_dict['documents'])
    for key in range(1, length):
     if 'document' in doc_dict['documents'][key]:
      if 'elements' in doc_dict['documents'][key]['document']:
       if 'category' in doc_dict['documents'][key]['document']['elements']:
        if 'categoryIds' in doc_dict['documents'][key]['document']['elements']['category']:
           category_object=doc_dict['documents'][key]['document']['elements']['category']['categoryIds']
           if len(category_object)==1:
              r=category_object[0]      
              y=catg_df[(catg_df.classification == "category") & (catg_df.id == r)]
              if y.empty==False:
                catg_df.loc[catg_df.id == r, 'counter'] += 1                    
                doc_dict['documents'][key]['document']['elements']['category']['path']=y['path'].iloc[0]
    sg.popup("Message:","=======","The merge of the category's key [path] into the documents JSON file is successfully done.") 
    return 
 else: 
     sg.popup("Message:Internal variables are empty.","========","Please upload first your ReST-APIs Data.")  
     return

#TASK2
def count_doc_per_category(df):
 try:
   new_df= pd.DataFrame()
   new_df = df[['id', 'path', 'counter']].copy()
   new_df.columns  = ["category", "path", "documents"]
   new_df = new_df.sort_values(['documents'], ascending=False)
   new_df=new_df.reset_index(drop=True)
   new_df.to_json('Classified_Documents_By_Category.json', orient='index', indent=2)
   fix_backslash_issue('Classified_Documents_By_Category.json')
   return True
 except:
    return False

#PySimpleGui Windows
def GUI_windows_handling(window, layout2):
 while True:
    event, values = window.read()
    if event == sg.WINDOW_CLOSED:
     break
    # ------ Task1.a  ------ #
    if event == 'Task1.a':
      if values['-lp1-']!='' and values['-lp2-']!='':
         if URL_Validator(values['-lp1-'])==True and URL_Validator(values['-lp2-'])==True:
           df,RestAPI_documents=get_json_from_api(values['-lp1-'],values['-lp2-'])
           window['-lp2-'].update('')
           window['-lp1-'].update('')
         else:
          sg.popup("Message: Invalid URL(s)", "=====================", "Please add valid ReST-APIs URLs or you might simply click on Default-URLs option from the menu list above.") 
      else:
        sg.popup("Message: Empty TextBox(s)", "=====================", "Please add manually the ReST-APIs URLs or you might just simply click on Default-URLs option from the menu list above.") 
    
    # ------ Task1.b  ------ #
    if event == 'Task1.b':
     if (df.empty==False) and (bool(RestAPI_documents)==True):
        merge_path_with_document(RestAPI_documents, df)
     else:
         sg.popup("Message:","========","Internal variables are empty. Please upload first your ReST-APIs Data.")
         
    # ------ Task2  ------ #
    if event == 'Task2':
      if (df.empty==False):
        if count_doc_per_category(df)==True:
            window2 = sg.Window('Message', layout2,size=(400, 120))   
            event, values = window2.read()
            if event == 'Open the file':
             os.system('notepad.exe {}'.format(r'Classified_Documents_By_Category.json'))   
             window2.close()
            elif event == 'Later on':
             window2.close()
        else:
            sg.popup("Message:","========","Sorry! Something went wrong...\n PLease reload the application.")
      else:
         sg.popup("Message:","========","Internal variables are empty. Please upload first your ReST-APIs Data.")

    # ------ All taks at once ------ #
    if event == 'All tasks as one single data pipeline':
        if values['-lp1-']!='' and values['-lp2-']!='':
         if URL_Validator(values['-lp1-'])==True and URL_Validator(values['-lp2-'])==True:
           df,RestAPI_documents=get_json_from_api(values['-lp1-'],values['-lp2-'])
           if (df.empty==False) and (bool(RestAPI_documents)==True):
               merge_path_with_document(RestAPI_documents, df)
               if count_doc_per_category(df)==True:
                  window2 = sg.Window('Message', layout2,size=(400, 120))   
                  event, values = window2.read()
                  if event == 'Open the file':
                       os.system('notepad.exe {}'.format(r'Classified_Documents_By_Category.json'))   
                       window2.close()
                  elif event == 'Later on':
                       window2.close()
               else:
                sg.popup("Message:","========","Sorry! Something went wrong...\n PLease reload the application.")
           else:
              sg.popup("Message:","========","Internal variables are empty. Please upload first your ReST-APIs Data.")
           window['-lp2-'].update('')
           window['-lp1-'].update('')
         else:
          sg.popup("Message: Invalid URL(s)", "=====================", "Please add valid ReST-API urls OR you might simply click on Default-URLs option from the menu list above.") 
        else:
          sg.popup("Message: Empty TextBox(s)", "=====================", "Please add manually the ReST-API urls OR you might just simply click on Default-URLs option from the menu list above.") 
        
    # ------ Proces menu choices ------ #
    if event == 'Task-Explaination':
        window.disappear()
        sg.popup('Tasks explaination is as follow:',"=======================",
                     'Task1.a: Permit to load both input JSON Objects from the given ReST APIs.\n',
                     'Task1.b: Permit to merge the values of the path property provided by the category objects (2nd API call) into the “category” element of the document objects (1er API call).\n',
                     'Task2: Permit to write into a JSON file the number of documents per category path in the data set. With JSON format: {category: category_id, path: path, documents: count}.\n',
                     'All: Operates as a simple data “pipeline” that extracts JSON data from the input ReST API (task1.a), transforms the data (task1.b), stores the data and performs a simple evaluation on that data (task2).\n',grab_anywhere=True)
        window.reappear()
    elif event == 'Default-URLs':
        doc_api = "https://content-us-1.content-cms.com/api/06b21b25-591a-4dd3-a189-197363ea3d1f/delivery/v1/search?q=classification:content&fl=document:%5bjson%5d&fl=type&rows=100"
        cag_api = "https://content-us-1.content-cms.com/api/06b21b25-591a-4dd3-a189-197363ea3d1f/delivery/v1/search?q=classification:category&rows=100"
        window['-lp1-'].update(doc_api)
        window['-lp2-'].update(cag_api)
    elif event == 'Exit':
        break    
 
# ------ Main driver ------ #
if __name__ == "__main__":
 sg.theme('Light Blue 2')      # Add some color to the window
 menu_def = [['&Option', ['&Default-URLs', '&Task-Explaination', '&Exit']],]
 font = ("Calibri Light", 12)
 layout1 = [
    [sg.Menu(menu_def, tearoff=False)],
    [sg.Text('Please enter your input data:',font=(13),pad=(2,15))],
    [sg.Text('Documents URL:', size=(13, 1)), sg.InputText(key="-lp1-", size=(140, 1),do_not_clear=False)],
    [sg.Text('Category URL:', size=(13, 1)), sg.InputText(key="-lp2-",size=(140, 1),do_not_clear=False)],
    [sg.Button('Task1.a',size=(15,1),font=font, pad=(65,20)), sg.Button('Task1.b',size=(15,1),font=font,pad=(0,20)), sg.Button('Task2',size=(15,1),pad=(65,20))],
    [sg.Button('All tasks as one single data pipeline',size=(65,1),font=font, pad=(65,0))]
]
 layout2 = [[sg.Text('Message:\n =======\n The output JSON file is successfully generated.', pad=(3,8))],
           [sg.Button('Open the file'), sg.Button('Later on')]]
 window = sg.Window('Code-Assignement_JSON-Data-Streamer', layout1,size=(655, 230))   
 try:
  GUI_windows_handling(window, layout2)
  window.close()
 except Exception as e:
  sg.popup("Message:","========",e)
  window.close()
         