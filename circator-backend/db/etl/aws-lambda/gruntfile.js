module.exports = function(grunt) {
  grunt.loadNpmTasks('grunt-aws-lambda');
  grunt.loadNpmTasks('grunt-writefile');
  grunt.loadNpmTasks('grunt-jscs');
  grunt.initConfig({
    jscs: {
      src: './*.js',
      options: {
        config: '.jscsrc',
        esnext: false,
        verbose: true,
        requireCurlyBraces: [],
      },
    },
    writefile: {
      options: {
        data: {
          dbhost: process.env.DB_HOST,
          dbname: process.env.DB_NAME,
          dbuser: process.env.DB_USER,
          dbpass: process.env.DB_PASS,
        },
      },
      index: {
        src: 'build/env.hbs',
        dest: '.env',
      },
    },
    lambda_invoke: {
      default: {
        options: {
        },
      },
    },
    lambda_package: {
      default: {
        options: {
          include_files: ['.env', './assets/**'],
        },
      },
    },
    lambda_deploy: {
      default: {
        arn: process.env.DEPLOY_ARN,
        options: {},
      },
    },
  });

  grunt.registerTask('check', ['jscs']);

  grunt.registerTask('run', ['check', 'lambda_invoke']);
  grunt.registerTask('run-nochecks', ['lambda_invoke']);

  grunt.registerTask('config', ['writefile']);

  grunt.registerTask('build-nochecks', ['config', 'lambda_package']);
  grunt.registerTask('build', ['check', 'build-nochecks']);

  grunt.registerTask('deploy-nochecks', ['build-nochecks', 'lambda_deploy']);
  grunt.registerTask('deploy', ['build', 'lambda_deploy']);
};