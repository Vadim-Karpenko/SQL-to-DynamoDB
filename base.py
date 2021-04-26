import sqlparse, boto3, traceback, re, itertools
from utils import clean_identifiers_quotes, nested_rename, getFromDict
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr


class SQLtoDynamo:
    """
        Parse SQL syntax to generate DynamoDB query
    """
    def __init__(self, region='us-east-1'):
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.fields_to_rename = []
        self.is_regex_required = False
        self.regexp_condition_string = ""
        self.regexp_condition_list = []
        
        self.clean = lambda x: x.strip('\"').strip("\'")
        
    def parse_query(self, query):
        # parse query using sqlparse library and remove all whitespaces between commands
        raw_parsed = sqlparse.parse(query)[0]
        return [i for i in raw_parsed if not i.ttype == sqlparse.tokens.Whitespace]
        
    def execute(self, query):
        parsed = self.parse_query(query)
        # if command equal to select'
        command = parsed[0].value.lower()
        object_to_transform = parsed[1].value.lower()
        if command == 'select':
            # then parse it as "select" command
            result = self.parse_select(parsed)
        elif command == 'alter':
            # dynamodb does not support this functionality
            result = []
        elif command == 'create' and object_to_transform == 'database':
            # you need to create it manualy
            result = []
        elif command == 'drop' and object_to_transform == 'database':
            result = []
        elif command == 'backup' and object_to_transform == 'database':
            result = []
        elif command == 'create' and object_to_transform == 'table':
            pass
        return result
    
    def _replace_str_index(self, text, index=0, replacement=''):
        return '%s%s%s'%(text[:index],replacement,text[index+1:])

    def field_to_rename(self, field):
        data_to_check = field.split(' ')
        for index, value in enumerate(data_to_check):
            if value.lower() == 'as':
                self.fields_to_rename.append((data_to_check[index+1], data_to_check[index-1])) 
                print((data_to_check[index+1], data_to_check[index-1]))
                return data_to_check[index+1]
    
    def parse_identifiers(self, identifiers, as_string=True):
        result = []
        is_have_wildcart = False
        for identifier in identifiers.value.split(","):
            identifier = identifier.strip()
            if identifier == "*":
                result = []
                is_have_wildcart = True
            else:
                if ' as ' in identifier.lower():
                    if not is_have_wildcart:
                        result.append(self.field_to_rename(identifier))
                    else:
                        self.field_to_rename(identifier)
                else:
                    if not is_have_wildcart:
                        result.append(identifier)
        if as_string:
            return ', '.join(clean_identifiers_quotes(result))
        return clean_identifiers_quotes(result)
    
    def convert_value(self, value):
        
        # try to convert sqlparse object to int
        try:
            value = Decimal(int(value.value))
        except:
            value = self.clean(str(value.value))
        return value

    
    def parse_conditions(self, conditions):
        # remove whitespaces
        conditions = [i for i in conditions.flatten() if not i.ttype == sqlparse.tokens.Whitespace][1:]
        self.regexp_condition_string = ' '.join([i.value for i in conditions])
        ExpressionAttributeValues = {}
        FilterExpression = " ".join([i.value for i in conditions])
        for index, value in enumerate(conditions):
            if value.ttype in [sqlparse.tokens.String.Symbol, sqlparse.tokens.Number.Integer]:
                operator = conditions[index-1]
                variable = conditions[index-2]
                new_variable_name = ":var" + str(len(ExpressionAttributeValues)+1)
                
                if operator.value in ['=','>=','<=','!=','>','<','<>']:
                    self.regexp_condition_string = self.regexp_condition_string.replace(variable.value + ' ' + operator.value + ' ' + value.value, 'True')
                    
                    ExpressionAttributeValues[new_variable_name] = self.convert_value(value)
                    conditions[index].value = new_variable_name
                    if operator.value == '!=':
                        operator.value = '<>'
                        
                    FilterExpression = " ".join([i.value for i in conditions])
                elif operator.value.lower() == 'like':
                    exp_value = self.clean(value.value)
                    like_value = self.clean(value.value)
                    is_not = False
                    
                    if variable.value.lower() == 'not':
                        variable = conditions[index-3]
                        is_not = True
                        operator.value = conditions[index-2].value + " " + operator.value
                        
                    regex_str = exp_value
                    if like_value[0] == '%':
                        regex_str = self._replace_str_index(regex_str, 0, "(?!^)")
                    elif like_value[0] == '_':
                        regex_str = self._replace_str_index(regex_str, 0, "^.")
                    if like_value[-1] == '%':
                        regex_str = self._replace_str_index(regex_str, len(regex_str)-1, "(?!$)")
                    elif like_value[-1] == '_':
                        regex_str = self._replace_str_index(regex_str, len(regex_str)-1, ".$")

                    print(regex_str)
                    #exp_value = value.value.replace("%", "(.*)").replace("_", ".")
                    regex_str = self.clean(regex_str.replace("%", "(.*)").replace("_", "."))
                    self.regexp_condition_list.append((variable.value, regex_str, "{} {} {}".format(variable, operator, value.value), is_not))
                    value_to_save = ''

                    if exp_value[-1] == '%' and exp_value[0] == '%' and not ('%' in exp_value[1:-1] or '_' in exp_value):
                        if is_not:
                            FilterExpression = FilterExpression.replace("{} {} {}".format(variable, operator, value.value),"not contains({}, {})".format(variable, new_variable_name))
                        else:
                            FilterExpression = FilterExpression.replace("{} {} {}".format(variable, operator, value.value),"contains({}, {})".format(variable, new_variable_name))

                        value_to_save = self.convert_value(value).strip('%')
                    elif exp_value[-1] == '%' and not ('%' in exp_value[1:-1] or '_' in exp_value):
                        if is_not:
                            FilterExpression = FilterExpression.replace("{} {} {}".format(variable, operator, value.value),"not begins_with({}, {})".format(variable, new_variable_name))
                        else:
                            FilterExpression = FilterExpression.replace("{} {} {}".format(variable, operator, value.value),"begins_with({}, {})".format(variable, new_variable_name))
                        value_to_save = self.convert_value(value).strip('%')
                    else:
                        splitted_values = []
                        for splitted_value in re.split("_|%", exp_value):
                            if splitted_value:
                                new_variable_name = ":var" + str(len(ExpressionAttributeValues)+1)
                                ExpressionAttributeValues[new_variable_name] = splitted_value
                                if is_not:
                                    splitted_values.append('not contains({}, {})'.format(variable, new_variable_name))
                                else:
                                    splitted_values.append('contains({}, {})'.format(variable, new_variable_name))
                        
                        filter_string = ' and '.join(splitted_values)
                        
                        FilterExpression = FilterExpression.replace("{} {} {}".format(variable, operator, value.value), filter_string)
                        #value_to_save = self.convert_value(value).strip('%')

                    if value_to_save:
                        ExpressionAttributeValues[new_variable_name] = value_to_save
                            
                        
        return FilterExpression, ExpressionAttributeValues
    
    def rename_result(self, result_data):
        for r_field in self.fields_to_rename:
            for item in result_data['Items']:
                nested_rename(item, [self.clean(i) for i in r_field[0].split('.')], self.clean(r_field[1]))
                
    def check_by_regex(self, result_data):
        result_items = list(result_data['Items'])
        result_to_return = []
        for index, result in enumerate(result_items):
            eval_string = self.regexp_condition_string
            for regex_condition in self.regexp_condition_list:
                path_to_variable = regex_condition[0].split('.')
                if regex_condition[3]:
                    if not re.search(regex_condition[1], getFromDict(result, path_to_variable)):
                        eval_string = eval_string.replace(regex_condition[2], 'True')
                    else:
                        eval_string = eval_string.replace(regex_condition[2], 'False')
                else:
                    if re.search(regex_condition[1], getFromDict(result, path_to_variable)):
                        eval_string = eval_string.replace(regex_condition[2], 'True')
                    else:
                        eval_string = eval_string.replace(regex_condition[2], 'False')
            if eval(eval_string, {'__builtins__':{}}):
                result_to_return.append(result)
            else:
                result_data['Count'] -= 1
                result_data['regex_excluded'] += 1
        result_data['Items'] = result_to_return
        return result_data
            
    def get_result(self, table, **kwargs):
        # delete all not valid keys from kwargs
        for key in list(kwargs.keys()):
            if not kwargs[key]:
                del kwargs[key]
        result = table.scan(**kwargs)
        data_result = result
        while 'LastEvaluatedKey' in result:
            if 'Limit' in kwargs and kwargs['Limit'] > result['Count'] and result['LastEvaluatedKey'] and kwargs['Limit']:
                result = table.scan(ExclusiveStartKey=result['LastEvaluatedKey'], **kwargs)
                data_result['Items'].extend(result['Items'])
                data_result['Count'] += result['Count']
                data_result['ScannedCount'] += result['ScannedCount']
                if 'LastEvaluatedKey' in result:
                    data_result['LastEvaluatedKey'] = result['LastEvaluatedKey']
                else:
                    del data_result['LastEvaluatedKey']
                kwargs['Limit'] -= result['Count']
            elif not 'Limit' in kwargs and result['LastEvaluatedKey'] and kwargs['Limit']:
                result = table.scan(ExclusiveStartKey=result['LastEvaluatedKey'], **kwargs)
                data_result['Items'].extend(result['Items'])
                data_result['Count'] += result['Count']
                data_result['ScannedCount'] += result['ScannedCount']
                if 'LastEvaluatedKey' in result:
                    data_result['LastEvaluatedKey'] = result['LastEvaluatedKey']
                else:
                    del data_result['LastEvaluatedKey']
            else:
                break
        
        data_result['regex_excluded'] = 0
        if self.regexp_condition_list and self.regexp_condition_string:
            data_result = self.check_by_regex(data_result)
        if self.fields_to_rename:
            self.rename_result(data_result)
        return data_result
    
    def get_identifiers(self, parsed):
        result = []
        for i in parsed[1:]:
            if i.ttype == sqlparse.tokens.Keyword and i.value.lower() == 'from':
                return sqlparse.sql.IdentifierList(result)
            else:
                if i.ttype == sqlparse.tokens.Keyword and i.value.lower() == 'as':
                    i.value = ' AS '
                result += list(i.flatten())
                
    def get_from_index(self, parsed):
        for key, value in enumerate(parsed):
            if value.value.lower() == 'from':
                return key
            
    def get_limit(self, parsed):
        if parsed[-2].ttype == sqlparse.tokens.Keyword and parsed[-2].value.lower() == 'limit':
            try:
                limit = int(parsed[-1].value)
            except:
                raise Exception("Limit value is not valid")
            return limit
        else:
            return None
        
    def parse_select(self, parsed):
        # get string of all attributes what we want to receive in response
        AttributesToGet = self.parse_identifiers(self.get_identifiers(parsed))
        from_index = self.get_from_index(parsed)
        FilterExpression = None
        ExpressionAttributeValues = None
        # get from which table we want to receive a response
        table_name = self.parse_identifiers(parsed[from_index+1], as_string=False)[0]
        table = self.dynamodb.Table(table_name)
        # if table name is not end of the string
        if parsed[from_index+1] != parsed[-1]:
            # if after table name is WHERE
            if parsed[from_index+2].__class__ == sqlparse.sql.Where:
                # parse conditions and tranform them to DynamoDB syntax
                FilterExpression, ExpressionAttributeValues = self.parse_conditions(parsed[from_index+2])
            else:
                raise Exception("An error occured with WHERE keyword, are you sure it is correct?")
            
        result = self.get_result(table,
                                 ProjectionExpression=AttributesToGet,
                                 FilterExpression=FilterExpression,
                                 ExpressionAttributeValues=ExpressionAttributeValues,
                                 Limit=self.get_limit(parsed))
        return result
