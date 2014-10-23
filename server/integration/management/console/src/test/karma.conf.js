'use strict';

module.exports = function(config) {

  config.set({
    basePath : '..', //!\\ Ignored through gulp-karma //!\\

    files : [ //!\\ Ignored through gulp-karma //!\\
        'src/main/bower_components/angular/angular.js',
        'src/main/bower_components/angular/angular-route.js',
        'src/main/bower_components/angular-mocks/angular-mocks.js',
        'src/main/{webapp,components}/** /*.js',
        'src/test/unit/** /*.js'
    ],

    autoWatch : false,

    frameworks: ['jasmine'],

    browsers : ['PhantomJS'],

    plugins : [
        'karma-phantomjs-launcher',
        'karma-jasmine'
    ]
  });

};
