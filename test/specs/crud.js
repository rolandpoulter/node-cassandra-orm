module.exports = require('spc').describe('CRUD:', function () {
	var fixtures = require('../fixtures'),
	    schema = require('../schema'),
	    User = schema.User;

	before(function () {
		// TODO: why does the call in ../index.js not work
		should();
	});

	describe('a user', function () {
		beforeEach(function () {
			this.subject = new User({
				email:    this.email    = 'email',
				name:     this.name     = 'name',
				bio:      this.bio      = 'bio',
				age:      this.age      = 25,
			});
		});

		it('should have an id', function () {
			this.subject.getId().should.be.ok;
		});

		it('should have an email', function () {
			this.subject.data.email.should.equal(this.email);
		});

		it('should have a name', function () {
			this.subject.data.name.should.equal(this.name);
		});

		it('should have a bio', function () {
			this.subject.data.bio.should.equal(this.bio);
		});

		it('should have an age', function () {
			this.subject.data.age.should.equal(this.age);
		});

		it('should not have been approved', function () {
			expect(this.subject.data.approved).to.be.false;
		});

		it('should have set joined at to now', function () {
			var joinedAt = this.subject.data.joinedAt,
			    now = Date.now();

			joinedAt.should.be.within(now - 50, now + 50);
		});

		describe('when saved', function () {
			beforeEach(function (end) {
				var that = this;

				this.subject.save(function (err) {
					if (err) return end(err);

					User.byId(that.subject.getId(), function (err, user) {
						that.user = user;
						end(err);
					});
				});
			});

			it('should persist', function () {
				expect(this.user).to.be.ok;
			});

			it('should store id', function () {
				this.user.getId().toString().should.equal(this.subject.getId().toString());
			});

			it('should store email', function () {
				this.user.data.email.should.equal(this.subject.data.email);
			});

			it('should store name', function () {
				this.user.data.name.should.equal(this.subject.data.name);
			});

			it('should store bio', function () {
				this.user.data.bio.should.equal(this.subject.data.bio);
			});

			it('should store age', function () {
				this.user.data.age.should.equal(this.subject.data.age);
			});

			it('should store approved', function () {
				this.user.data.approved.should.equal(this.subject.data.approved);
			});

			it('should store joinedAt', function () {
				this.user.data.joinedAt.toString().should.equal(new Date(this.subject.data.joinedAt).toString());
			});

			describe('and updated', function () {
				beforeEach(function (end) {
					var that = this;

					this.subject.update({
						name: this.name = 'Roland',
						bio:  this.bio  = 'Failure'

					}, function (err) {
						if (err) return end(err);

						User.byId(that.subject.getId(), function (err, user) {
							that.user = user;
							end(err);
						});
					});
				});

				it('should change the name', function () {
					this.subject. data.name.should.equal(this.name);
					this.user.    data.name.should.equal(this.name);
				});

				it('should change the bio', function () {
					this.subject. data.bio.should.equal(this.bio);
					this.user.    data.bio.should.equal(this.bio);
				});
			});

			describe('and destroyed', function () {
				beforeEach(function (end) {
					var that = this;

					this.subject.destroy(function (err) {
						if (err) return end(err);

						User.byId(that.subject.getId(), function (err, user) {
							that.user = user;
							end(err);
						})
					});
				});

				it('should only have an id', function () {
					// NOTE: Cassandra doesn't delete rows right away, so
					//       there is no real way to confirm that a row has been deleted.

					var properties = Object.keys(this.user.data);

					properties.length.should.equal(1);
					properties[0] === 'id';
				});
			});
		});
	});

	describe('query user(s)', function () {
		before(function (end) {
			fixtures.save('users', end);
		});

		describe('find', function () {
			beforeEach(function (end) {
				var that = this;

				User.find({}, function (err, users) {
					if (err) return end(err);

					that.users = users;
					end();
				});
			});

			it('should include all saved rows', function () {
				
			});
		});
	});
});
