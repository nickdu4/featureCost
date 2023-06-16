import pandas as pd
import openpyxl
from NavTools import nav_connect as nv
from datetime import date
import streamlit as st

template = pd.DataFrame({'A':['PricingRequestID','****',None, 'Standard Feature','****', None,'StdAG','****'], 
                         'B':['Model','****',None, None,None, None,'StdFAV','****'],
                         'C':[None,None,None, None,None, None,None,None], 
                         'D':[None,None,None, 'Optional Feature','****', None,'OptAG','****'],
                         'E':[None,None,None, None,None, None,'OptFAV','****']})

def append_value(dict_obj, key, value):
    # Check if key exist in dict or not
    if key in dict_obj:
        # Key exist in dict.0
        # Check if type of value of key is list or not
        dict_obj[key].append(value)
    else:
        # As key is not in dict,                                                       
        # so, add key-value pair
        dict_obj[key] = value

def hadoopSend(finalDF, stdOpp):
    odbc4 = nv.odbc()
    if stdOpp == 1:
        try:
            row, columns = finalDF.shape
            insertText3 = '''INSERT INTO dufourlab.optionalFeatCost VALUES'''
            for index, row in finalDF.iterrows():
                print('blammo')
                insertText3+=f'''('{row['Price Request ID']}', '{row['cost_run_date']}', '{row['plant_cd']}','{row['Model']}', '{row['Standard_Feature']}', '{row['Optional_Feature']}', '{row['Installation']}', '{row['Assembly']}', '{row['Variation']}','{row['AG']}', '{row['part_number']}', '{row['part_count']}', '{row['currency']}', '{row['part_cost_individual']}', '{row['part_cost_sum']}', '{row['part_full_desc']}'),'''
            insertText3 = insertText3[:-1]
            odbc4.read_sql(insertText3)

            odbc4.close()
        except TypeError:
            pass
    if stdOpp == 0:
        try:
            row, columns = finalDF.shape
            insertText3 = '''INSERT INTO dufourlab.standardFeatCost VALUES'''
            for index, row in finalDF.iterrows():
                print('bang')
                insertText3+=f'''('{row['Price Request ID']}', '{row['cost_run_date']}', '{row['plant_cd']}','{row['Model']}', '{row['Standard_Feature']}', '{row['Optional_Feature']}', '{row['Installation']}', '{row['Assembly']}', '{row['Variation']}','{row['AG']}', '{row['part_number']}', '{row['part_count']}', '{row['currency']}', '{row['part_cost_individual']}', '{row['part_cost_sum']}', '{row['part_full_desc']}'),'''
            insertText3 = insertText3[:-1]
            odbc4.read_sql(insertText3)

            odbc4.close()
        except TypeError:
            pass

def favQueryFunct(favDict, favQuery):
    if len(favDict) == 1:
        singleFAV = list(favDict.keys())[0]

        singleFAVQuery = f'''SELECT DISTINCT plant_cd, fav, part_number, part_count, item_no, seq_no, currency, part_cost_individual, part_cost_sum, contract_message
        from analytics_prod.fav_cost_current
        WHERE fav = '{str(singleFAV)}' '''

        AGdfA = pd.DataFrame.from_dict(favDict.items())
        AGdfA.rename(columns={0:'faving', 1:'AG'}, inplace=True)
        odbc1 = nv.odbc()
        prePreAGmergeA = odbc1.read_sql(singleFAVQuery)
        odbc1.close()

    else:
        AGdfA = pd.DataFrame.from_dict(favDict.items())
        AGdfA.rename(columns={0:'faving', 1:'AG'}, inplace=True)
        odbc1 = nv.odbc()
        prePreAGmergeA = odbc1.read_sql(favQuery)
        odbc1.close()
    
    prePreAGmergeA.loc[prePreAGmergeA['contract_message'] == '(NO CONTRACT PRICE)', 'part_cost_sum'] = '****No Contract Price****'
    prePreAGmergeA.loc[prePreAGmergeA['contract_message'] == '(NO CONTRACT PRICE)', 'part_cost_individual'] = '****No Contract Price****' 

    dfA = prePreAGmergeA.merge(AGdfA, how='inner', left_on='fav', right_on='faving')

    return dfA

def plantQueryAG(favDF, plantQuery, fetrCode, standardFeature, mdl):
    partScrapeA = tuple(favDF['part_number'].tolist())

    if len(partScrapeA) == 1:
        descriptionsA = f'''
        SELECT part_no, part_full_desc from cdms.part
        WHERE part_no = {partScrapeA}
        '''
    else:
        descriptionsA = f'''
        SELECT part_no, part_full_desc from cdms.part
        WHERE part_no in {partScrapeA}
        '''
    
    odbc2 = nv.odbc()
    decQueryA = odbc2.read_sql(descriptionsA)
    odbc2.close()

    odbc3 = nv.odbc()
    plantDFa = odbc3.read_sql(plantQuery)
    odbc3.close()

    dFAVa = favDF.merge(decQueryA, how='inner', left_on='part_number', right_on='part_no')
    prePreDroppedA = dFAVa.drop(columns=['faving'])
    droppedSharedA = prePreDroppedA.drop(columns=['part_no'])
    plantDFa = droppedSharedA.loc[droppedSharedA['plant_cd'] == plantDFa.iloc[0,0]]

    # favA df formatting for join
    plantDFa.insert(1, 'Installation', plantDFa['fav'].str[:10])
    plantDFa.insert(2, 'Assembly', plantDFa['fav'].str[10:12])
    plantDFa.insert(3, 'Variation', plantDFa['fav'].str[12:])
    plantDFa.insert(1,column='Model',value=mdl)
    plantDFa.insert(2,column='Standard_Feature',value=standardFeature)
    plantDFa.insert(3,column='Optional_Feature',value=fetrCode)
    preSortDFa = plantDFa.drop('fav', axis=1)

    final = preSortDFa.sort_values(by=['Installation'])
    finalA = pd.DataFrame(final)
    return finalA

def formatToExcel(result, stdOpp):
    if stdOpp == 1:
        result.to_csv("control.csv", index = False)
    
    if stdOpp == 0:
        result.to_csv("standardCost.csv", index = False)
    
    return result

def costFixFunct(missingPartList, resultDF):
    if len(missingPartList) != 0:
        costFixOdbc = nv.odbc()

        for i in missingPartList:
            
            peopleSoft = f'''
            with mainTab as (

            SELECT *, SUBSTRING(peoplesoft.ps_zi_cntr_itm_dtl.cntrct_id, 6, 1) AS cntrct_type, to_timestamp(cntrct_begin_dt,'yyyy-MM-dd HH:mm:ss.s') contract_bg_date

            ,to_timestamp(cntrct_expire_dt,'yyyy-MM-dd HH:mm:ss.s') contract_end_date
            ,to_timestamp(effdt,'yyyy-MM-dd HH:mm:ss.s') effective_date     
            FROM peoplesoft.ps_zi_cntr_itm_dtl
            WHERE cntrct_expire_dt > now()
            )

            select inv_item_id, business_unit, price_cntrct_base, adj_amt from mainTab

            where
            inv_item_id = '{i}'
            and
            cntrct_status = 'A'
            and
            line_status = 'O'
            '''

            newCostDF = costFixOdbc.read_sql(peopleSoft)
            plantCode = resultDF['plant_cd'].max()
            
            #filter for business_unit here
            print(resultDF.loc[resultDF['part_number'] == i, 'part_count'].values)
            newCostDF['actualTotal'] = newCostDF['price_cntrct_base'] + newCostDF['adj_amt']
            resultDF.loc[resultDF['part_number'] == i, 'part_cost_individual'] = newCostDF['actualTotal'].max()
            resultDF.loc[resultDF['part_number'] == i, 'part_cost_sum'] = newCostDF['actualTotal'].max() #* int(resultDF.loc[resultDF['part_number'] == i, 'part_count'].values)

        
        costFixOdbc.close()

    else:
        pass

def fileSkim(file, preSliceStndrdFeat):
        heading = openpyxl.load_workbook(file)
        pageView = heading.active

        priceRequestID = pageView["A2"].value
        standFeature = pageView["A5"].value

        strdData = pd.read_excel(file, header=6)

        model = pageView["B2"].value

        plantQueryA = f'''
        with checkOne as(
        select 
        b.mdl_7_cd, b.cmpny_loc_cd ,a.fetr_cd ,count(*) as feat_cnt  
        from va_feature_search_prod.tora_9_job_fetr as a left join 
        va_feature_search_prod.tora_2_ord as b on a.ord_dte_key_srl_no = b.ord_dte_key_srl_no 
        left join va_feature_search_prod.tora_1_job as c 
        on a.ord_dte_key_srl_no = c.ord_dte_key_srl_no  
        where 
        b.ord_dte > date_sub(now(), interval 2 years)  
        group by b.cmpny_loc_cd ,a.fetr_cd, b.mdl_7_cd),

        otherOne as(select a.mdl_7_cd, a.fetr_cd, max(a.feat_cnt) as feat_cnt_max  from checkOne as a
        group by a.fetr_cd, a.mdl_7_cd )

        ,otherOtherOne as (
        select distinct 
        b.cmpny_loc_cd, b.fetr_cd, a.mdl_7_cd  from otherOne as a left join checkOne as b  
        on a.fetr_cd = b.fetr_cd and a.feat_cnt_max = b.feat_cnt  

        where
        a.mdl_7_cd = '{model}'
        and
        b.fetr_cd = '{standFeature}'
        )

        select cmpny_loc_cd, count(*) as num
        from otherOtherOne
        group by cmpny_loc_cd
        order by count(*) desc
        limit 1;
        '''

        for i in strdData.iloc():
            if i[0] == 'Standard Feature':
                break
            else:
                if type(i[1]) == float or pd.isnull(i[1]):
                    break
                else:
                    append_value(preSliceStndrdFeat, i[1], i[0])

        stdFeatFavDict = dict(list(preSliceStndrdFeat.items()))

        StdTupleFAV = tuple(stdFeatFavDict)

        favQueryStd = f'''SELECT DISTINCT plant_cd, fav, part_number, part_count, item_no, seq_no, currency, part_cost_individual, part_cost_sum, contract_message
        from analytics_prod.fav_cost_current
        WHERE fav in {StdTupleFAV} '''

        if 'keys' not in st.session_state:
            stdFAV = favQueryFunct(stdFeatFavDict, favQueryStd)

            standardDone = plantQueryAG(stdFAV, plantQueryA, standFeature, standFeature, model)
            contractFindStd = standardDone.loc[standardDone['part_cost_individual'] == 'No Contract Price']
            noCostStandard = list(contractFindStd['part_number'])
            
            # if len(noCostStandard) != 0:
            #     costFixFunct(noCostStandard, standardDone)
            # else:
            #     pass
            
            for row in pageView.rows:
                
                preSliceOptFeat = {}
                if row[3].value == "Optional Feature":
                    
                    headerAdjust = row[3].row + 2 
                    rowFeat = row[3].row + 1
                    optData = pd.read_excel(file, header= headerAdjust)
                    rowFeat = str(rowFeat)
                    rowFeat = str("D"+ rowFeat)
                    optionalFeature = pageView[rowFeat]

                    for ii in optData.iloc():
                            if ii[3] == 'Optional Feature':
                                break
                            else:
                                fav = str(ii[4])
                                ag = str(ii[3])
                                if fav == 'nan':
                                    pass
                                else:
                                    append_value(preSliceOptFeat, fav, ag)

                    AtupleFAV = tuple(preSliceOptFeat)

                    favQueryA = f'''SELECT DISTINCT plant_cd, fav, part_number, part_count, item_no, seq_no, currency, part_cost_individual, part_cost_sum, contract_message
                    from analytics_prod.fav_cost_current
                    WHERE fav in {AtupleFAV} '''

                    ############################################################ Optional Feature Excel Format and Run(s) ###################################################################

                    dfFAV = favQueryFunct(preSliceOptFeat, favQueryA)
                    doneDone = plantQueryAG(dfFAV, plantQueryA, optionalFeature.value, standFeature, model)         
                    contractFind = doneDone.loc[doneDone['part_cost_individual'] == 'No Contract Price']
                    #noCostPartList = list(contractFind['part_number'])
                    #costFixFunct(noCostPartList, doneDone)
                    
                    #Making Dataframe price columns into strings for editing within streamlit
                    doneDone = doneDone.astype({"part_cost_individual": str})
                    doneDone = doneDone.astype({"part_cost_sum": str})
                    doneDone.insert(0,column='Price Request ID',value= priceRequestID)
                    doneDone.insert(1, column='cost_run_date', value= date.today())
                    doneDone = doneDone.astype({"cost_run_date": str})
                    
                    standardDone = standardDone.astype({"part_cost_individual": str})
                    standardDone = standardDone.astype({"part_cost_sum": str})

                    standardDone.insert(0,column='Price Request ID',value= priceRequestID)
                    standardDone.insert(1, column='cost_run_date', value= date.today())
                    standardDone = standardDone.astype({"cost_run_date": str})

                    optionalDone = doneDone.sort_values(by=['Installation', 'Assembly','Variation', 'AG', 'item_no', 'seq_no'])
                    standDone = standardDone.sort_values(by=['Installation', 'Assembly','Variation', 'AG', 'item_no', 'seq_no'])
                    
                    return optionalDone, standDone 
                
def dfDiffCheck(newCompare,oldResult):
    
    matched = pd.merge(newCompare, oldResult, on=['Price Request ID', 'cost_run_date', 'plant_cd', 'Model', 'Standard_Feature', 'Optional_Feature', 'Installation', 'Assembly', 'Variation', 'part_number', 'part_count', 'item_no', 'seq_no', 'currency', 'part_cost_individual', 'part_cost_sum', 
    'contract_message', 'AG', 'part_full_desc'], how='inner')

    # identify non-matching rows in df1
    df1_only = pd.merge(newCompare, oldResult, on=['Price Request ID', 'cost_run_date', 'plant_cd', 'Model', 'Standard_Feature', 'Optional_Feature', 'Installation', 'Assembly', 'Variation', 'part_number', 'part_count', 'item_no', 'seq_no', 'currency', 'part_cost_individual', 'part_cost_sum', 
    'contract_message', 'AG', 'part_full_desc'], how='left', indicator=True)
    df1_only = df1_only[df1_only['_merge'] == 'left_only']
    #df1_only = df1_only.drop('_merge', axis=1)

    # identify non-matching rows in df2
    df2_only = pd.merge(newCompare, oldResult, on=['Price Request ID', 'cost_run_date', 'plant_cd', 'Model', 'Standard_Feature', 'Optional_Feature', 'Installation', 'Assembly', 'Variation', 'part_number', 'part_count', 'item_no', 'seq_no', 'currency', 'part_cost_individual', 'part_cost_sum', 
    'contract_message', 'AG', 'part_full_desc'], how='right', indicator=True)
    df2_only = df2_only[df2_only['_merge'] == 'right_only']
    #df2_only = df2_only.drop('_merge', axis=1)

    # combine matching and non-matching rows
    result = pd.concat([matched, df1_only, df2_only], ignore_index=True)
    # result.to_excel("both1.xlsx")
    # result.to_excel("both2.xlsx")