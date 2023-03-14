require([
    "lib/codemirror", "javascript"
  ], function(CodeMirror) {
    CodeMirror.fromTextArea(document.getElementById("code"), {
      lineNumbers: true,
      mode: "javascript",
      continuousScanning: 500,
      lineNumbers: true,
    });
  });

function runJsCode() {
    requirejs(['./mixpanel.amd'], function(mixpanel) { 
        var code = document.getElementById("code").value;
        eval(code);  
        document.getElementById("status").style.visibility="visible";
    });
}

function statusChange() {
    document.getElementById("status").style.visibility="hidden";
}

