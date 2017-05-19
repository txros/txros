dockerNode(image: 'uf-mil:txros') {
	stage("Checkout") {
		checkout scm
		sh '''
			ln -s $PWD ~/catkin_ws/src/txros
			git submodule update --init --recursive
		'''
	}
	stage("Build") {
		sh '''
			source /opt/ros/kinetic/setup.bash > /dev/null 2>&1
			catkin_make -C ~/catkin_ws -B
		'''
	}
	stage("Test") {
		sh '''
			source /opt/ros/kinetic/setup.bash > /dev/null 2>&1
			source ~/catkin_ws/devel/setup.bash > /dev/null 2>&1
			trial txros
		'''
	}
	stage("Format") {
		sh '''
			if [[ ! -z "$(python2.7 -m flake8 --max-line-length=120 --exclude=__init__.py .)" ]]; then
				echo "The preceding Python following files are not formatted correctly"
				exit 1
			fi
		'''
	}
}
