On Linux Centos 7, the default font for the 'monspace' font family is not monospaced.

This can be verified in the following way:
```shell
fc-match monospace
```
By default this shows:
```
DejaVuSans.ttf: "DejaVu Sans" "Book"
```
This font family is not monospaced.

This results in the tables in the legenda of the graphs looking chaotic.

## Switch monospace font
The 'monospace' font family can be switched to a monospaced font in the following way:

- Install a monospaced font:
```shell
sudo yum install liberation-mono-fonts.noarch
```
- Create a file `.config/fontconfig/fonts.conf`, and add the following contents:
```xml
<?xml version='1.0'?>
<!DOCTYPE fontconfig SYSTEM 'fonts.dtd'>
<fontconfig>
  <alias>
    <family>monospace</family>
    <prefer><family>Liberation Mono</family></prefer>
  </alias>
</fontconfig>
```


