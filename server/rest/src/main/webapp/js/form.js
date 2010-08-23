// form; check for mandatory fields
function checkMandatories(formName)
    {
    var theForm=document[formName];
    var m=theForm.mgnlMandatory;
    var i=0;
    var ok=true;
    alertText = theForm.mgnlFormAlert.value;
    if (m)
        {
        if (!m[0])
            {
            var tmp=m;
            m=new Object();
            m[0]=tmp;
            }
        while (m[i])
            {
            var name=m[i].value;
            var type;
            var mgnlField;
            if(document.all) mgnlField=theForm(name);
            else mgnlField=theForm[name];

            if (mgnlField.type) type=mgnlField.type;
            else if (mgnlField[0] && mgnlField[0].type) type=mgnlField[0].type

            switch (type)
                {
                case "select-one":
                    if (mgnlField.selectedIndex==0) ok=false;
                    break;
                case "checkbox":
                case "radio":
                    var obj=new Object();
                    if (!mgnlField[0]) obj[0]=mgnlField;
                    else obj=mgnlField;
                    var okSmall=false;
                    var ii=0;
                    while (obj[ii])
                        {
                        if (obj[ii].checked)
                            {
                            okSmall=true;
                            break;
                            }
                        ii++;
                        }
                    if (!okSmall) ok=false;
                    break;
                default:
                    if (!mgnlField.value) ok=false;
                }
            if (!ok)
                {
                alert(alertText);
                if (!mgnlField[0]) mgnlField.focus();
                return false;
                }
            i++;
            }
        }
    if (ok) return true;
    else return false;
    }