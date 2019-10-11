# Unit Testing Demo


## Sample input dataset

```
+---------+-----+-----+
|    col_a|col_b|col_c|
+---------+-----+-----+
|      aaa|  zfo|  duh|
| grmpxgoh|  tjf|  seh|
|   hwqbjn|  vdg|  jqh|
|   vkzgnj|  tga|  lpo|
|evsaivnjg|  qzh|  fpc|
|adgueifcv|  yec|  vkm|
|      wah|  kmb|  oth|
|  umohsxu|  xcm|  dpm|
|itdkcwfke|  ynm|  ykd|
+---------+-----+-----+
```

## Processing

The example processing code enhances the input dataset by adding 23 
new columns, `col_d` through to `col_z`: 

- `col_d` is dependent in `col_a`. If `col_a` is equal to "aaa", then 
`col_d` will get the value "AAA". Otherwise `col_d` will be set to `BBB`.
- `col_e` is an integer, the literal value `-99`
- `col_f` is an integer, the length of the value in `col_a`
- `col_g` through to `col_z` are all literal strings with random values. 