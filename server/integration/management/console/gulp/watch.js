'use strict';

var gulp = require('gulp');

gulp.task('watch', ['wiredep', 'styles'] ,function () {
  gulp.watch('src/main/{webapp,components}/**/*.less', ['styles']);
  gulp.watch('src/main/{webapp,components}/**/*.js', ['scripts']);
  gulp.watch('src/main/assets/images/**/*', ['images']);
  gulp.watch('bower.json', ['wiredep']);
});
