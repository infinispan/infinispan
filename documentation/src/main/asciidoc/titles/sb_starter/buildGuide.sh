# Build the guide

# Find the directory name and full path
CURRENT_GUIDE=${PWD##*/}
CURRENT_DIRECTORY=$(pwd)
RED='\033[0;31m'
BLACK='\033[0;30m'
# Do not produce pot/po by default
L10N=0
# A comma separated lists of default locales that books should be translated to.
# This can be overriden in each book's individual buildGuide.sh
LANG_CODE=ja-JP

usage(){
  cat <<EOM
USAGE: $0 [OPTION]

DESCRIPTION: Build the documentation in this directory and (optionally)
the associated L10N pot/po files.

OPTIONS:
  -h       Print help.
  -t       Produce the L10N pot/po files for this documentation

EOM
}

while getopts "ht:" c
 do
     case "$c" in
       t)         L10N=1
                  LANG_CODE=$OPTARG;;
       h)         usage
                  exit 1;;
       \?)        echo "Unknown option: -$OPTARG." >&2
                  usage
                  exit 1;;
     esac
done

if [ $L10N -eq 1 ]; then
   echo "Building pot/po for $CURRENT_GUIDE"
   ccutil translate --langs $LANG_CODE
fi

# Remove the html and build directories and then recreate the html/images/ directory
if [ -d html ]; then
   rm -r html/
fi
if [ -d build ]; then
   rm -r build/
fi

mkdir -p html
cp -r ../../docs/topics/images/ html/

echo ""
echo "********************************************"
echo " Building $CURRENT_GUIDE                "
echo "********************************************"
echo ""
echo "Building an asciidoctor version of the $CURRENT_GUIDE"
asciidoctor -t -dbook -a toc -o html/$CURRENT_GUIDE.html master.adoc

echo "Building the ccutil version of the $CURRENT_GUIDE"
ccutil compile --lang en_US --main-file master.adoc --format html-single

cd ..

echo "$CURRENT_GUIDE (AsciiDoctor) is located at: " file://$CURRENT_DIRECTORY/html/$CURRENT_GUIDE.html

if [ -d  $CURRENT_DIRECTORY/build/tmp/en-US/html-single/ ]; then
  echo "$CURRENT_GUIDE (ccutil) is located at: " file://$CURRENT_DIRECTORY/build/tmp/en-US/html-single/index.html
  exit 0
else
  echo -e "${RED}Build of $CURRENT_GUIDE failed!"
  echo -e "${BLACK}See the log above for details."
  exit 1
fi
