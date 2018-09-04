---
name: Bug report
about: Create a report to help us improve

---

**Describe the bug**

A clear and concise description of what the bug is.

**Schema**

Describe your table schema:
* Primary key / composite key
* Any index?

**Debug info**

Show the query that you're having trouble with by copy-pasting the result of:

```php
print_r($query->toDynamoDbQuery());
```

**Version info**

* Laravel: 5.5
* laravel-dynamodb: latest
