Libretto loads fixture data into your mongodb database. 

Exporting:

```bash
libretto export /path/to/fixtures/dir --database=app-testing
```

Importing:

```bash
libretto import "/path/to/fixtures/dir/*" --database=app-testing
```

Using in tests:


```javascript
var exec = require("child_process").exec;

describe("test#", function() {

  //load the fixtures
  before(function(next) {
    exec("./node_modules/.bin/libretto import " + __dirname + "/fixtures/scenario1/* --database=app-testing", next);
  });

  it("do some test", function(next) {

  });

})
```
