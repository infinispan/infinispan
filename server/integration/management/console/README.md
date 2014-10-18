# Installation

- Install node.js and npm
- Install gulp globally (use sudo if needed): `npm install gulp -g`
- Enter root directory (contains package.json):
  - run `npm install` to instal local packages, they will be installed to /node_modules/
  - run `bower install` to install client dependencies, they will be installed to /src/main/bower_components/
  - NOTE: you will need to repeat this two steps if there are some changes to package.json or bower.json

# Running web application

- We assume you are positioned in application root
- Run locally in development mode by running `gulp serve` - tab in browser will automatically open.
- Run locally in production mode by running `gulp serve:dist`
- Build app for production by running `gulp build` - result will be stored to /dist/
- Run unit tests by running `gulp test`
- Run e2e tests by running `gulp protractor`
- Run e2e tests on production code by running `gulp protractor:dist`
- Run `gulp clean` to remove generated files like /dist/ and /.tmp/
- Run `gulp clear-cache` to clean gulp cache

# Setting up the server (we need it for web application to fetch data, you do this only once)
- you need JDK 7 or 8 (Oracle) and Maven
- from top-level of infinispan run: `./build.sh clean package -DskipTests`
- move to /infinispan/server/integration/build/target/infinispan-server-7.0.0-SNAPSHOT
- run `./bin/add-user.sh` to create new user
  - chose (a) - Management for type of user
  - type `admin` as username
  - choose (a) to update user
  - type `!qazxsw2` as password
  - leave blank for groups to which user will belong to
  - choose `yes` for AS processes

# Usual flow for development
- run server by going to /infinispan/server/integration/build/target/infinispan-server-7.0.0-SNAPSHOT and running `./bin/standalone.sh`
- go to /infinispan/server/integration/management/console and run `gulp serve`