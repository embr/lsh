lsh
---

a pure python locality senstive hashing implementation

### Installation

### Usage
```python

from lsh import LSHCache

cache = LSHCache()
    
docs = [
  "lipstick on a pig",
  "you can put lipstick on a pig",
  "you    can put lipstick on a pig but it's still a pig",
  "you can put lipstick on a pig it's still a pig",
  "i think they put some lipstick on a pig but it's still a pig",
  "putting lipstick on a pig",
  "you know you can put lipstick on a pig",
  "they were going to send us binders full of women",
  "they were going to send us binders of women",
  "a b c d e f",
  "a b c d f"]

dups = {}
for i, doc in enumerate(docs):
    dups[i] = cache.insert(doc.split(), i)
    ...
````

### Roadmap
* add more tests
* add `save()` and `from_file()` methods
* rewrite with redis backend?
