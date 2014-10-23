'use strict';

var gulp = require('gulp');

// inject bower components
gulp.task('wiredep', function () {
  var wiredep = require('wiredep').stream;

  gulp.src('src/main/{webapp,components}/*.scss')
    .pipe(wiredep())
    .pipe(gulp.dest('src/main'));

  gulp.src('src/main/*.html')
    .pipe(wiredep({
      exclude: ['bower_components/bootstrap']
    }))
    .pipe(gulp.dest('src/main'));
});
