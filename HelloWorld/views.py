from django.http import HttpResponse
from django.shortcuts import render
from django.db import connection
from django.views.decorators.csrf import csrf_exempt
import datetime
import json
import re
cursor = connection.cursor()
def sel_stock_list(cursor,sql):
    # sql = 'select distinct Z.stock_id,Z.stock_name,Z.zhuang_grade,I.h_table,I.bk_name from com_zhuang Z '\
    #                'left join stock_informations I '\
    #                'on Z.stock_id = I.stock_id '\
    #                'where zhuang_grade >= 1000 and zhuang_grade <10000 '
    # sql = 'select distinct Z.stock_id,Z.stock_name,Z.redu_5,I.h_table,I.bk_name from com_redu_test Z '\
    #                'left join stock_informations I '\
    #                'on Z.stock_id = I.stock_id '\
    #                'where redu_5 >= 10000 and trade_date = "2021-04-02" order redu_5 DESC '
    cursor.execute(sql)
    res = cursor.fetchall()
    # res = [('000617','中油资本',111,'7','test'),] * 100
    # id_list = []
    # for tup in res:
    #     id_list.append(tup[0])
    # print('id:',id_list)
    return res
def sel_stock_k_date(res,date_e = None,date_s = '2020-08-01'):
    data_list = []
    for stock in res:
        # print('stock:',stock)
        id = stock[0]
        h_tab = stock[3]
        # print('id:',id)
        if date_e == None:
            sql = 'select date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,0,0,0,0  '\
                       ' from stock_history_trade{0} where stock_id = "{1}" and trade_date > "{2}"'.format(h_tab,id,date_s)
        else:
            sql = 'select date_format(trade_date ,"%Y-%m-%d") as trade_date,open_price,close_price,low_price,high_price,turnover_rate,0,0,0,0  '\
                       ' from stock_history_trade{0} where stock_id = "{1}" and trade_date > "{2}" and trade_date < "{3}"'.format(h_tab,id,date_s,date_e)
        cursor.execute(sql)
        rows = cursor.fetchall()
        # print('rows:',rows)
        rows = list(rows)
        for i in range(len(rows)):
            rows[i] = list(rows[i])
        info = str(stock[0]) +' '+ str(stock[1]) +' ' + str(stock[2])+' ' + str(stock[4])
        tag = stock[0]
        rows_list = [tag,info,rows]
        data_list.append(rows_list)
    return data_list
        # print('row:',rows[i])
        # print(rows)
def del_stock(stock_info='',reson='',db_field= '',db_table = ''):
    if stock_info[0:6]=='reason':
        stock_flag = stock_info[6:]
    else:
        print('input error:{}'.format(stock_info))
    sql = "delete from {0} where {1} = '{2}'".format(db_table,db_field,stock_flag)
    print('sql:',sql)
    cursor.execute(sql)
def hello(request):
    return HttpResponse('hello world!')
@csrf_exempt
def runoob(request):
    context = {}
    context['data'] = []
    if request.method == "POST":
        # put_reson = json.loads(request.body)
        print('req:',request.POST)
        remen_xiaoboxin_param_dict = {}
        zhuang_param_dict = {}
        remen_5_param_dict = {}
        for key in request.POST:
            print('value:',key,request.POST[key])
            # del_stock(stock_info=key, reson=request.POST[key], db_field='stock_id', db_table='com_zhuang')
            if key == 'monitor_input':
                sql = ""
                res = sel_stock_list(cursor,sql)
                print('res:',res)
                data_list = sel_stock_k_date(res)
                print('data_list_len:', len(data_list))
                context['data'] = data_list  # [['2015-10-16',18.4,18.58,18.33,18.79,67.00,1,0.04,0.11,0.09],['2015-10-19',18.56,18.25,18.19,18.56,55.00,0,-0.00,0.08,0.09]]
                # context['hello'] = 'hello world!'
                # return render(request,'echarts_value_g.html',context)
                # return render(request, 'test_page.html', context)
            elif key in ('remen_xiaoboxin_B_input_date_s','remen_xiaoboxin_B_input_date_e','remen_xiaoboxin_B_today_input',
                         'remen_xiaoboxin_B_input_grade_s','remen_xiaoboxin_B_input_grade_e'):
                remen_xiaoboxin_param_dict[key] = request.POST[key]
                
            elif key in ('zhuang_input_date_s', 'zhuang_input_date_e',
                             'zhuang_input_grade_s', 'zhuang_input_grade_e'):
                zhuang_param_dict[key] = request.POST[key]
                
            elif key in ('remen_5_date_s','remen_5_date_e','remen_5_today_input',
                         'remen_5_grade_s','remen_5_grade_e'):
                remen_5_param_dict[key] = request.POST[key]
        if len(remen_xiaoboxin_param_dict) != 0:
            sql = 'select distinct Z.stock_id,Z.stock_name,Z.zhuang_grade,I.h_table,I.bk_name from com_zhuang Z '\
                           'left join stock_informations I '\
                           'on Z.stock_id = I.stock_id '\
                           'where zhuang_grade >= "{0}" and zhuang_grade <"{1}" '.format(remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_grade_s'],
                                                                                         remen_xiaoboxin_param_dict['remen_xiaoboxin_B_input_grade_e'])
            res = sel_stock_list(cursor, sql)
            data_list = sel_stock_k_date(res,date_s=remen_xiaoboxin_param_dict['zhuang_input_date_s'],date_e=remen_xiaoboxin_param_dict['zhuang_input_date_e'])
            context['data'] = data_list
        elif len(zhuang_param_dict) != 0:
            sql = 'select distinct Z.stock_id,Z.stock_name,Z.zhuang_grade,I.h_table,I.bk_name from com_zhuang Z '\
                           'left join stock_informations I '\
                           'on Z.stock_id = I.stock_id '\
                           'where zhuang_grade >= "{0}" and zhuang_grade <"{1}" '.format(zhuang_param_dict['zhuang_input_grade_s'],
                                                                                         zhuang_param_dict['zhuang_input_grade_e'])
            res = sel_stock_list(cursor, sql)
            data_list = sel_stock_k_date(res,date_s=zhuang_param_dict['zhuang_input_date_s'],date_e=zhuang_param_dict['zhuang_input_date_e'])
            context['data'] = data_list
        elif len(remen_5_param_dict) != 0:
            sql = 'select distinct Z.stock_id,Z.stock_name,Z.zhuang_grade,I.h_table,I.bk_name from com_zhuang Z '\
                           'left join stock_informations I '\
                           'on Z.stock_id = I.stock_id '\
                           'where zhuang_grade >= "{0}" and zhuang_grade <"{1}" '.format(zhuang_param_dict['zhuang_input_grade_s'],
                                                                                         zhuang_param_dict['zhuang_input_grade_e'])
            res = sel_stock_list(cursor, sql)
            data_list = sel_stock_k_date(res,date_s=zhuang_param_dict['zhuang_input_date_s'],date_e=zhuang_param_dict['zhuang_input_date_e'])
            context['data'] = data_list
        # reason = request.POST['reason']
        # re_res = re.findall('.*?reson(.*?)=(.*?)',bytes(request.body,encoding = "utf-8").decode())
        # print('re_res:',re_res)
    return render(request,'echarts_value_g.html',context)
    # return render(request, 'test_page.html', context)
# def receive(request):
#     if request.POST:
#         print(request.POST.body)
#         reason = request.POST['reason']
#         # cursor.execute(up)