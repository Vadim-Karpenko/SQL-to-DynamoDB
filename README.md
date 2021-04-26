# SQLtoDynamo
Translate the SQL query into the DynamoDB query.

# How to use
### Example 1
```python
parser = SQLtoDynamo()
result = parser.execute('SELECT pk, last_name FROM Person WHERE last_name LIKE "%k%" LIMIT 1')#  
```
### Result
```python
{
  'Items': [
    {
      'last_name': 'Karpenko',
      'pk': Decimal('2')
    }
  ],
  'Count': 1,
  'ScannedCount': 1,
  'LastEvaluatedKey': {
    'pk': Decimal('2')
  },
  'ResponseMetadata': {
    'RequestId': 'J6F6EGCHIKSR8R4V9DA7512TI7VV4KQNSO5AEMVJF66Q9ASUAAJG',
    'HTTPStatusCode': 200,
    'HTTPHeaders': {
      'server': 'Server',
      'date': 'Mon, 26 Apr 2021 14:27:36 GMT',
      'content-type': 'application/x-amz-json-1.0',
      'content-length': '120',
      'connection': 'keep-alive',
      'x-amzn-requestid': 'J6F6EGCHIKSR8R4V9DA7512TI7VV4KQNSO5AEMVJF66Q9ASUAAJG',
      'x-amz-crc32': '2591339689'
    },
    'RetryAttempts': 0
  },
  'regex_excluded': 0
}
```

### Example 2

```python
parser = SQLtoDynamo()
result = parser.execute('SELECT pk, personal_data.birth_day FROM Person WHERE pk = 1')
```

### Result
```python
{
  'Items': [
    {
      'pk': Decimal('1'),
      'personal_data': {
        'birth_day': '27'
      }
    }
  ],
  'Count': 1,
  'ScannedCount': 2,
  'ResponseMetadata': {
    'RequestId': 'RAHTLBV47356HSBF0FJTEGJEP3VV4KQNSO5AEMVJF66Q9ASUAAJG',
    'HTTPStatusCode': 200,
    'HTTPHeaders': {
      'server': 'Server',
      'date': 'Mon, 26 Apr 2021 14:39:40 GMT',
      'content-type': 'application/x-amz-json-1.0',
      'content-length': '102',
      'connection': 'keep-alive',
      'x-amzn-requestid': 'RAHTLBV47356HSBF0FJTEGJEP3VV4KQNSO5AEMVJF66Q9ASUAAJG',
      'x-amz-crc32': '2428572807'
    },
    'RetryAttempts': 0
  },
  'regex_excluded': 0
}
```
