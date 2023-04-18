version: 2
jobs: 
   one: 
        docker
            -image: cimg/ruby: 2.6.8 
             auth:
                  username: mydockerhub-user
                  password: $DOCKERHUB_PASSWORD
steps:
   - checkout 
   - run: echo "A first hello" This prints "A first hello" to stdout.
   - run: sleep 25
two:
     docker:
          - image: cimg/ruby: 3.0.2
            auth:
                 username: mydockerhub-user
                 password: $DOCKERHUB PASSWORD
steps:
   - checkout
   - run: echo "A more familiar hi"
   - run: sleep 15
workflows:
     version: 2
     one_and_two:
         jobs: 
            -one
            -two
